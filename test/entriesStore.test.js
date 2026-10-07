// entriesStore on real MongoDB (see helpers/db.js: npm run test:db).
const { load } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

let store, Key, Values, Entries

before(async () => {
    await db.setup(__filename)
    store = load("utils/entriesStore.js")
    Key = load("api/models/Key.js")
    Values = load("api/models/Value.js")
    Entries = load("api/models/Entries.js")
})
after(db.teardown)
beforeEach(db.clean)

const byKeyValue = (a, b) => String(a.key ?? "").localeCompare(String(b.key ?? "")) || String(a.value ?? "").localeCompare(String(b.value ?? ""))
const sorted = async (Model, filter) => (await db.docs(Model, filter)).sort(byKeyValue)

describe("writeAccumulator", () => {
    test("creates keys, values and entries with refs to the origin", async () => {
        await store.writeAccumulator({ city: { Rome: ["public-data"], Milan: ["public-data"] } }, "https://api/a")

        assert.deepEqual(await sorted(Entries), [
            { key: "city", value: "Milan", visibility: ["public-data"], refs: [{ origin: "https://api/a", visibility: "public-data" }] },
            { key: "city", value: "Rome", visibility: ["public-data"], refs: [{ origin: "https://api/a", visibility: "public-data" }] }
        ])
        assert.deepEqual(await db.docs(Key), [
            { key: "city", visibility: ["public-data"], refs: [{ origin: "https://api/a", visibility: "public-data" }] }
        ])
        assert.equal((await db.docs(Values)).length, 2)
    })

    test("key visibility is the union over its values, value visibility the union over its keys", async () => {
        await store.writeAccumulator({
            k1: { v: ["a@b.it"] },
            k2: { v: ["public-data"], w: ["a@b.it"] }
        }, "minio://bucket/file.json")

        const [key2] = await db.docs(Key, { key: "k2" })
        assert.deepEqual(key2.visibility.sort(), ["a@b.it", "public-data"])
        const [valueV] = await db.docs(Values, { value: "v" })
        assert.deepEqual(valueV.visibility.sort(), ["a@b.it", "public-data"])
        assert.deepEqual(valueV.refs.map(r => r.visibility).sort(), ["a@b.it", "public-data"])
    })

    test("is idempotent: writing the same block twice changes nothing", async () => {
        const acc = { k: { v: ["public-data"] } }
        await store.writeAccumulator(acc, "o1")
        const first = [await sorted(Entries), await sorted(Key), await sorted(Values)]
        await store.writeAccumulator(acc, "o1")
        assert.deepEqual([await sorted(Entries), await sorted(Key), await sorted(Values)], first)
    })

    test("a second origin adds its ref to the same document", async () => {
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "o1")
        await store.writeAccumulator({ k: { v: ["PILOT SHARED Data"] } }, "o2")
        const entries = await db.docs(Entries)
        assert.equal(entries.length, 1)
        assert.deepEqual(entries[0].refs, [
            { origin: "o1", visibility: "public-data" },
            { origin: "o2", visibility: "PILOT SHARED Data" }
        ])
        assert.deepEqual(entries[0].visibility, ["public-data", "PILOT SHARED Data"])
    })

    test("legacy documents without refs are never matched: a new document with refs is created", async () => {
        await Entries.collection.insertOne({ key: "k", value: "v", visibility: ["public-data"] })
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "o1")
        const entries = await db.docs(Entries)
        assert.equal(entries.length, 2)
        assert.equal(entries.filter(e => e.refs).length, 1)
        assert.deepEqual(entries.find(e => !e.refs), { key: "k", value: "v", visibility: ["public-data"] }) // untouched
    })

    test("unordered bulk writes in chunks of 1000 operations", async t => {
        const bulkWrite = t.mock.method(Entries.collection, "bulkWrite")
        const acc = { k: {} }
        for (let i = 0; i < 2500; i++)
            acc.k["v" + i] = ["public-data"]
        await store.writeAccumulator(acc, "o1")
        assert.deepEqual(bulkWrite.mock.calls.map(c => c.arguments[0].length), [1000, 1000, 500])
        assert.ok(bulkWrite.mock.calls.every(c => c.arguments[1].ordered === false))
        assert.equal(await Entries.collection.countDocuments(), 2500)
    })

    test("per-document write errors are logged, not thrown", async t => {
        t.mock.method(Entries.collection, "bulkWrite", async () => {
            throw Object.assign(new Error("bulk"), { writeErrors: [{ errmsg: "E11000 duplicate key" }] })
        }, { times: 1 })
        await assert.doesNotReject(store.writeAccumulator({ k: { v: ["public-data"] } }, "o1"))
        assert.equal((await db.docs(Key)).length, 1) // keys and values are still written
    })

    test("connection errors are thrown", async t => {
        t.mock.method(Entries.collection, "bulkWrite", async () => { throw new Error("connection lost") }, { times: 1 })
        await assert.rejects(store.writeAccumulator({ k: { v: ["public-data"] } }, "o1"), /connection lost/)
    })

    test("creates the indexes used by the upserts and by removeOrigin", async () => {
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "o1")
        const names = async Model => (await Model.collection.indexes()).map(i => Object.keys(i.key).join("+"))
        assert.ok((await names(Entries)).includes("key+value"))
        for (const Model of [Entries, Key, Values])
            assert.ok((await names(Model)).includes("refs.origin"), Model.modelName)
        for (const Model of [Entries, Key, Values])
            assert.ok((await Model.collection.indexes()).every(i => !i.unique || i.name == "_id_"), "no unique indexes")
    })
})

describe("removeOrigin", () => {
    test("deletes documents referenced only by the origin, keeps the shared ones with the other refs", async () => {
        await store.writeAccumulator({ shared: { v: ["public-data"] }, onlyA: { x: ["a@b.it"] } }, "A")
        await store.writeAccumulator({ shared: { v: ["PILOT SHARED Data"] } }, "B")

        await store.removeOrigin("A")

        assert.deepEqual(await db.docs(Entries), [
            { key: "shared", value: "v", visibility: ["PILOT SHARED Data"], refs: [{ origin: "B", visibility: "PILOT SHARED Data" }] }
        ])
        assert.deepEqual((await db.docs(Key)).map(k => k.key), ["shared"])
        assert.deepEqual(await db.docs(Values), [
            { value: "v", visibility: ["PILOT SHARED Data"], refs: [{ origin: "B", visibility: "PILOT SHARED Data" }] }
        ])
    })

    test("leaves legacy documents and other origins alone", async () => {
        await Key.collection.insertOne({ key: "legacy", visibility: ["public-data"] })
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "B")
        await store.removeOrigin("A")
        assert.deepEqual((await db.docs(Key)).map(k => k.key).sort(), ["k", "legacy"])
        assert.deepEqual((await db.docs(Key, { key: "legacy" }))[0], { key: "legacy", visibility: ["public-data"] })
    })

    test("remove + write rebuilds an origin (upsertRecords flow)", async () => {
        await store.writeAccumulator({ k: { old: ["public-data"] } }, "A")
        await store.removeOrigin("A")
        await store.writeAccumulator({ k: { new: ["public-data"] } }, "A")
        assert.deepEqual((await db.docs(Entries)).map(e => e.value), ["new"])
        assert.deepEqual((await db.docs(Values)).map(v => v.value), ["new"])
    })
})

describe("createCollector", () => {
    test("indexes items without their mongo _id and writes on flush", async () => {
        const collector = store.createCollector("o1")
        await collector.add([{ _id: "6650aa", name: "n1", n: 1 }])
        assert.equal(await Entries.collection.countDocuments(), 0) // nothing written before flush
        await collector.flush()
        assert.deepEqual((await db.docs(Entries)).map(e => e.key).sort(), ["n", "name"])
    })

    test("flushes automatically every 500 items, final flush writes the rest", async t => {
        const bulkWrite = t.mock.method(Entries.collection, "bulkWrite")
        const collector = store.createCollector("o1")
        await collector.add(Array.from({ length: 1200 }, (_, i) => ({ id: "i" + i })))
        assert.equal(bulkWrite.mock.callCount(), 2)
        await collector.flush()
        assert.equal(bulkWrite.mock.callCount(), 3)
        assert.equal(await Entries.collection.countDocuments(), 1200)
        await collector.flush() // nothing pending: no write
        assert.equal(bulkWrite.mock.callCount(), 3)
    })
})
