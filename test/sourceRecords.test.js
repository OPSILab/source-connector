// sourceRecords on real MongoDB + PostgreSQL (see helpers/db.js: npm run test:db).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

let records, Source, Orion, Entries, Key

before(async () => {
    await db.setup(__filename)
    records = load("utils/sourceRecords.js")
    const collections = load("utils/collections.js")
    Source = collections.collectionModel("api")
    Orion = collections.collectionModel("orion")
    Entries = load("api/models/Entries.js")
    Key = load("api/models/Key.js")
})
after(db.teardown)
beforeEach(db.clean)

const pgRows = (table = "sources") => db.query(`SELECT name, data, record FROM ${table} ORDER BY id`)

describe("makeItem / prepareBackupValues", () => {
    test("moves id to sourceId and sets source to the origin", () => {
        assert.deepEqual(records.makeItem({ id: 7, name: "n" }, "https://api/x"), { name: "n", source: "https://api/x", sourceId: 7 })
    })

    test("no id (e.g. the Orion datapoints): no sourceId field at all", () => {
        const item = records.makeItem({ name: "n" }, "https://api/x")
        assert.deepEqual(item, { name: "n", source: "https://api/x" })
        assert.ok(!("sourceId" in item))
        const other = records.makeItem({ sourceId: "abc" }, "https://api/x")
        assert.deepEqual(other, { sourceId_original: "abc", source: "https://api/x" })
    })

    test("orion: fromUrl = origin (an existing fromUrl kept in fromUrl_original), id and source untouched", () => {
        assert.deepEqual(records.makeItem({ id: 7, source: "EUROSTAT" }, "https://ds/x", "orion"), { id: 7, source: "EUROSTAT", fromUrl: "https://ds/x" })
        assert.deepEqual(records.makeItem({ fromUrl: "old" }, "https://ds/x", "orion"), { fromUrl: "https://ds/x", fromUrl_original: "old" })
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

    test("the connector's table, created when missing; nothing when its toPostgres is off", async () => {
        config.collections.orion.toPostgres = true
        config.collections.orion.postgres = "orion_rows"
        await records.insertToPostgre([{ name: "o" }], "orion", undefined, "https://ds/x", "orion")
        assert.deepEqual((await pgRows("orion_rows")).map(r => r.name), ["o"])
        await db.query("DROP TABLE orion_rows")
        config.collections.api.toPostgres = false
        await records.insertToPostgre([{ name: "a" }], "api", undefined, "https://api/x")
        assert.deepEqual(await pgRows(), [])
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
        assert.deepEqual((await db.docs(Entries, { key: "color" })).map(e => e.connectors), [["api"], ["api"]])
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

describe("per connector", () => {
    test("orion records: Orion collection, fromUrl, replaced by dataset url; the API collection untouched", async () => {
        await records.replaceRecords([{ name: "r1", k: "v1" }], "https://ds/x", { connector: "orion" })
        await records.replaceRecords([{ name: "r2", k: "v2" }], "https://ds/x", { connector: "orion" })
        const stored = await db.docs(Orion)
        assert.deepEqual(stored.map(({ dupl_hash, ...d }) => d), [{ name: "r2", k: "v2", fromUrl: "https://ds/x" }])
        assert.equal(typeof stored[0].dupl_hash, "string") // unique index of the Datapoint schema
        assert.deepEqual(await db.docs(Source), [])
        const [entry] = await db.docs(Entries, { key: "k" })
        assert.deepEqual([entry.value, entry.connectors], ["v2", ["orion"]])
        assert.deepEqual(await pgRows(), []) // orion: no PostgreSQL by default
        assert.ok((await Orion.collection.indexes()).some(i => JSON.stringify(i.key) == '{"fromUrl":1}'))
    })

    test("toMongo = false: PostgreSQL only, no keys / values / entries", async () => {
        config.collections.api.toMongo = false
        await records.replaceRecords([{ id: 1, k: "v" }], "https://api/a")
        assert.deepEqual(await db.docs(Source), [])
        assert.deepEqual(await db.docs(Entries), [])
        assert.equal((await pgRows()).length, 1)
    })

    test("openReplace: block by block, with the connector", async () => {
        const writer = await records.openReplace("https://ds/y", { connector: "orion" })
        await writer.add([{ a: 1 }])
        await writer.add([{ a: 2 }, { a: 2 }]) // the same record twice: one document (upsert by dupl_hash)
        assert.equal(await writer.close(), 3)
        assert.deepEqual((await db.docs(Orion)).map(d => d.a).sort(), [1, 2])
    })

    test("storeRecords / clearOrigin", async () => {
        await records.storeRecords([records.makeItem({ id: 1 }, "https://api/a")], "https://api/a")
        assert.equal((await db.docs(Source)).length, 1)
        assert.equal((await pgRows()).length, 1)
        await records.clearOrigin("https://api/a")
        assert.equal((await db.docs(Source)).length, 0)
        assert.equal((await pgRows()).length, 0)
    })
})
