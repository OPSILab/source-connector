// sourceRecords on real MongoDB + PostgreSQL (see helpers/db.js: npm run test:db).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

let records, Source, Entries, Key

before(async () => {
    await db.setup(__filename)
    records = load("utils/sourceRecords.js")
    Source = load("api/models/Models.js").Source
    Entries = load("api/models/Entries.js")
    Key = load("api/models/Key.js")
})
after(db.teardown)
beforeEach(db.clean)

const pgRows = () => db.query("SELECT name, data, record FROM sources ORDER BY id")

describe("makeItem / prepareBackupValues", () => {
    test("moves id to sourceId and sets source to the origin", () => {
        assert.deepEqual(records.makeItem({ id: 7, name: "n" }, "https://api/x"), { name: "n", source: "https://api/x", sourceId: 7 })
    })

    test("keeps a pre-existing source / sourceId field as *_original", () => {
        const item = records.makeItem({ id: 1, source: "upstream", sourceId: "abc" }, "https://api/x")
        assert.equal(item.source, "https://api/x")
        assert.equal(item.sourceId, 1)
        assert.equal(item.source_original, "upstream")
        assert.equal(item.sourceId_original, "abc")
    })

    test("an existing *_original string is concatenated, a non-string one is nested", () => {
        const item = { source: "b", source_original: "a" }
        records.prepareBackupValues(item, "source")
        assert.equal(item.source_original, "a | b")

        const nested = { source: { x: 1 }, source_original: { y: 2 } }
        records.prepareBackupValues(nested, "source")
        assert.deepEqual(nested.source_original, { original: { y: 2 }, source: { x: 1 } })
    })

    test("does nothing when the field is missing", () => {
        const item = { a: 1 }
        records.prepareBackupValues(item, "source")
        assert.deepEqual(item, { a: 1 })
    })
})

describe("insertToPostgre", () => {
    test("nothing for an empty batch", async () => {
        await records.insertToPostgre([], "api", undefined, "https://api/x")
        assert.deepEqual(await pgRows(), [])
    })

    test("one row per item: name (name, id, batch value), data, record.from", async () => {
        await records.insertToPostgre([{ name: "a", n: 1 }, { id: "b" }, {}], "api", "batch", "https://api/x")
        assert.deepEqual(await pgRows(), [
            { name: "a", data: { name: "a", n: 1 }, record: { from: "https://api/x" } },
            { name: "b", data: { id: "b" }, record: { from: "https://api/x" } },
            { name: "batch", data: {}, record: { from: "https://api/x" } }
        ])
    })

    test("disabled by queryOptions.SQLQuery = false", async () => {
        config.queryOptions.SQLQuery = false
        await records.insertToPostgre([{ name: "a" }], "api", undefined, "https://api/x")
        assert.deepEqual(await pgRows(), [])
    })
})

describe("replaceRecords", () => {
    test("replaces sources (Mongo and PostgreSQL) and entries of the origin, other origins untouched", async () => {
        await records.replaceRecords([{ id: 1, color: "red" }], "https://api/a")
        await records.replaceRecords([{ id: 9, color: "blue" }], "https://api/b")
        await records.replaceRecords([{ id: 2, color: "green" }], "https://api/a")

        const sources = (await db.docs(Source)).sort((x, y) => x.sourceId - y.sourceId)
        assert.deepEqual(sources, [
            { color: "green", source: "https://api/a", sourceId: 2 },
            { color: "blue", source: "https://api/b", sourceId: 9 }
        ])
        assert.deepEqual((await db.docs(Entries, { key: "color" })).map(e => e.value).sort(), ["blue", "green"])
        // "source" and "sourceId" are indexed too (they are fields of the stored record)
        assert.deepEqual((await db.docs(Entries, { key: "source" })).map(e => e.value).sort(), ["https://api/a", "https://api/b"])

        const rows = await pgRows()
        assert.deepEqual(rows.map(r => [r.record.from, r.data.color]).sort(), [["https://api/a", "green"], ["https://api/b", "blue"]])
    })

    test("PostgreSQL disabled: Mongo only", async () => {
        config.queryOptions.SQLQuery = false
        await records.replaceRecords([{ id: 1 }], "https://api/a")
        assert.deepEqual(await pgRows(), [])
        assert.equal((await db.docs(Source)).length, 1)
    })

    test("a single object is treated as one record", async () => {
        await records.replaceRecords({ id: "x", k: "v" }, "https://api/a")
        assert.equal((await db.docs(Source)).length, 1)
        assert.deepEqual((await db.docs(Entries, { key: "k" })).map(e => e.value), ["v"])
    })

    test("non-object payloads (e.g. XML) are stored as { raw, name } and not indexed", async () => {
        await records.replaceRecords("<xml/>", "https://orion/download", { sqlName: "dist-1" })
        const [stored] = await db.docs(Source)
        assert.equal(stored.raw, "<xml/>")
        assert.equal(stored.name, "dist-1")
        assert.equal(stored.source, "https://orion/download")
        assert.equal((await db.docs(Entries)).length, 0)
        assert.equal((await db.docs(Key)).length, 0)
        const [row] = await pgRows()
        assert.equal(row.name, "dist-1")
        assert.equal(row.data.raw, "<xml/>")
    })

    test("an empty result clears the origin", async () => {
        await records.replaceRecords([{ id: 1, k: "v" }], "https://api/a")
        await records.replaceRecords([], "https://api/a")
        assert.equal((await db.docs(Source)).length, 0)
        assert.equal((await db.docs(Entries)).length, 0)
        assert.deepEqual(await pgRows(), [])
    })
})
