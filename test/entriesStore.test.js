// entriesStore on real MongoDB (see helpers/db.js: npm run test:db).
const { load, config, resetConfig } = require("./helpers/env")
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
beforeEach(async () => {
    resetConfig()
    await db.clean()
})

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

describe("connectors", () => {
    test("refs carry the connector, `connectors` is their union", async () => {
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "https://api/a", {}, "api")
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "https://eurostat/x.xml", {}, "orion")
        for (const Model of [Entries, Key, Values]) {
            const [doc] = await db.docs(Model)
            assert.deepEqual(doc.connectors, ["api", "orion"])
            assert.deepEqual(doc.refs.map(r => r.connector), ["api", "orion"])
        }
        await assert.rejects(store.writeAccumulator({ k: { v: ["public-data"] } }, "o", {}, "ftp"), /Unknown connector/)
    })

    test("removeOrigin recomputes `connectors`", async () => {
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "https://api/a", {}, "api")
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "https://eurostat/x.xml", {}, "orion")
        await store.removeOrigin("https://eurostat/x.xml")
        assert.deepEqual((await db.docs(Entries))[0].connectors, ["api"])
    })

    test("removeRefs: legacy refs without connector, or the refs of some connectors; docs left without refs deleted", async () => {
        await store.writeAccumulator({ k: { legacy: ["public-data"] } }, "minio://b/f.json")
        await store.writeAccumulator({ k: { v: ["public-data"] } }, "https://api/a", {}, "api")
        await store.writeAccumulator({ k: { v: ["public-data"], o: ["public-data"] } }, "https://eurostat/x.xml", {}, "orion")
        await store.removeRefs({ withoutConnector: true })
        assert.deepEqual((await sorted(Entries)).map(e => e.value), ["o", "v"])
        await store.removeRefs({ connectors: ["orion"] })
        const entries = await sorted(Entries)
        assert.deepEqual(entries.map(e => [e.value, e.connectors]), [["v", ["api"]]])
        assert.deepEqual((await db.docs(Key))[0].connectors, ["api"])
        await store.removeRefs({}) // nothing asked: nothing removed
        assert.equal((await db.docs(Entries)).length, 1)
    })
})

describe("values not indexed (Orion)", () => {
    const datapoint = (n, extra = {}) => ({ survey: "NAMA", dimensions: ["Lovech", String(2019 + n)], value: 1000.5 + n, ...extra })

    test("orion: `value` has its key, no values / entries; the key says so with its origin", async () => {
        const collector = store.createCollector("https://eurostat/a.xml", "orion")
        await collector.add([datapoint(1), datapoint(2)])
        await collector.flush()
        assert.deepEqual(await db.docs(Entries, { key: "value" }), [])
        assert.deepEqual(await db.docs(Values, { value: "1001.5" }), [])
        const [key] = await db.docs(Key, { key: "value" })
        assert.deepEqual(key.valuesNotIndexed, ["https://eurostat/a.xml"])
        assert.deepEqual(key.visibility, ["public-data"])
        assert.deepEqual(key.connectors, ["orion"])
        assert.deepEqual((await sorted(Entries, { key: "dimensions" })).map(e => e.value), ["2020", "2021", "Lovech"])
    })

    test("other connectors keep their `value` indexed", async () => {
        const collector = store.createCollector("https://api/b", "api")
        await collector.add([{ value: 7, name: "not orion" }, datapoint(1)])
        await collector.flush()
        assert.deepEqual((await db.docs(Entries, { key: "value" })).map(e => e.value).sort(), ["1001.5", "7"])
        assert.equal((await db.docs(Key, { key: "value" }))[0].valuesNotIndexed, undefined)
    })

    test("orion.datapointsNotIndexed decides which fields ([] = all indexed)", async () => {
        config.orion.datapointsNotIndexed = ["region"]
        let collector = store.createCollector("o1", "orion")
        await collector.add([datapoint(1, { region: "LOVECH" })])
        await collector.flush()
        assert.deepEqual((await db.docs(Entries, { key: "region" })), [])
        assert.equal((await db.docs(Entries, { key: "value" })).length, 1)
        config.orion.datapointsNotIndexed = []
        collector = store.createCollector("o2", "orion")
        await collector.add([datapoint(1, { region: "LOVECH" })])
        await collector.flush()
        assert.equal((await db.docs(Entries, { key: "region" })).length, 1)
    })

    test("removeOrigin takes the origin out of valuesNotIndexed (the key stays while others use it)", async () => {
        for (const origin of ["A", "B"]) {
            const collector = store.createCollector(origin, "orion")
            await collector.add([datapoint(1)])
            await collector.flush()
        }
        await store.removeOrigin("A")
        assert.deepEqual((await db.docs(Key, { key: "value" }))[0].valuesNotIndexed, ["B"])
        await store.removeOrigin("B")
        assert.deepEqual(await db.docs(Key, { key: "value" }), [])
    })

    test("a key used by Orion and by other records: shared refs, only the Orion origins listed", async () => {
        let collector = store.createCollector("A", "orion")
        await collector.add([datapoint(1)])
        await collector.flush()
        collector = store.createCollector("https://api/b", "api")
        await collector.add([{ value: 7 }])
        await collector.flush()
        await store.removeOrigin("A")
        const [key] = await db.docs(Key, { key: "value" })
        assert.deepEqual([key.refs.map(r => r.origin), key.valuesNotIndexed, key.connectors], [["https://api/b"], [], ["api"]])
    })
})

describe("formats", () => {
    test("entriesFormat: where getEntries finds the entries of a document", () => {
        assert.equal(store.entriesFormat({ city: "Rome" }), "object")
        assert.equal(store.entriesFormat({ json: [{ city: "Rome" }] }), "json")
        assert.equal(store.entriesFormat({ csv: [{ city: "Rome" }] }), "csv")
        assert.equal(store.entriesFormat({ type: "FeatureCollection", features: [{ properties: { city: "Rome" } }] }), "geojson")
        assert.equal(store.entriesFormat(undefined), "object")
    })

    test("collector: one ref per format of the documents, `formats` is their union", async () => {
        const collector = store.createCollector("https://api/mixed", "api")
        await collector.add([
            { city: "Rome" },
            { type: "FeatureCollection", features: [{ properties: { city: "Rome", poi: "Colosseum" } }] },
            { csv: [{ city: "Milan" }] }
        ])
        await collector.flush()
        const rome = (await db.docs(Entries, { key: "city", value: "Rome" }))[0]
        assert.deepEqual(rome.formats.sort(), ["geojson", "object"])
        assert.deepEqual(rome.refs.map(r => r.format).sort(), ["geojson", "object"])
        assert.deepEqual((await db.docs(Entries, { key: "poi" }))[0].formats, ["geojson"])
        assert.deepEqual((await db.docs(Values, { value: "Milan" }))[0].formats, ["csv"])
        assert.deepEqual((await db.docs(Key, { key: "city" }))[0].formats.sort(), ["csv", "geojson", "object"])
        // the GeoJSON envelope is not indexed, only the properties of the features
        assert.deepEqual(await db.docs(Key, { key: "type" }), [])
    })

    test("removeOrigin / removeRefs recompute `formats`", async () => {
        await store.writeAccumulator({ city: { Rome: ["public-data"] } }, "https://api/a", {}, "api", "object")
        await store.writeAccumulator({ city: { Rome: ["public-data"] } }, "minio://b/f.csv", {}, "minio", "csv")
        await store.removeOrigin("minio://b/f.csv")
        assert.deepEqual((await db.docs(Entries, { key: "city" }))[0].formats, ["object"])
        await store.writeAccumulator({ city: { Rome: ["public-data"] } }, "minio://b/f.csv", {}, "minio", "csv")
        await store.removeRefs({ connectors: ["api"] })
        assert.deepEqual((await db.docs(Entries, { key: "city" }))[0].formats, ["csv"])
    })

    test("an unknown format is refused", async () => {
        await assert.rejects(store.writeAccumulator({ a: { b: ["public-data"] } }, "o", {}, "api", "xml"), /Unknown format/)
    })
})
