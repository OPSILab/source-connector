// rebuild (entries + PostgreSQL mirror) on real MongoDB + PostgreSQL (see helpers/db.js: npm run test:db).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const { Client } = require("pg")

let rebuild, locks, store, Source, Orion, Key, Values, Entries

before(async () => {
    await db.setup(__filename)
    rebuild = load("utils/rebuild.js")
    locks = load("utils/jobLocks.js")
    store = load("utils/entriesStore.js")
    const collections = load("utils/collections.js")
    Source = collections.collectionModel("api")
    Orion = collections.collectionModel("orion")
    Key = load("api/models/Key.js")
    Values = load("api/models/Value.js")
    Entries = load("api/models/Entries.js")
})
after(db.teardown)
beforeEach(async () => {
    await db.clean()
    locks.polling = false
    locks.rebuilding = false
})

// Inserted with the native driver: stored exactly as written (e.g. a nested `source` object)
const insertSources = docs => Source.collection.insertMany(docs.map(doc => ({ ...doc })))
const insertDatapoints = docs => Orion.collection.insertMany(docs.map(doc => ({ ...doc })))
let hashes = 0 // unique dupl_hash: the Datapoint schema has a unique index on it (upsertRecords)
const datapoint = (url, dims, value, extra = {}) => ({ source: "EUROSTAT", survey: "NAMA", fromUrl: url, dimensions: dims, value, dupl_hash: "h" + hashes++, ...extra })
const pgRows = (table = "sources") => db.query(`SELECT name, data, record FROM ${table} ORDER BY id`)
const insertPgRow = (name, record, table = "sources") => db.query(`INSERT INTO ${table} (name, data, record) VALUES ($1, $2, $3)`, [name, {}, record])

// Makes pg queries (all clients) fail when `when(sql, params)` is true; the others run normally
function failPgQueries(t, when) {
    const original = Client.prototype.query
    t.mock.method(Client.prototype, "query", function (sql, params, ...rest) {
        const text = typeof sql === "string" ? sql : sql?.text
        if (typeof text === "string" && when(text, params))
            return Promise.reject(new Error("injected failure: " + text.slice(0, 30)))
        return original.call(this, sql, params, ...rest)
    })
}

describe("validate", () => {
    test("modes, connector and origin", () => {
        assert.match(rebuild.validate({}), /mode is required/) // never "all" by default
        assert.match(rebuild.validate({ mode: "" }), /mode is required/)
        assert.equal(rebuild.validate({ mode: "all" }), undefined)
        assert.equal(rebuild.validate({ mode: "all", connector: "orion" }), undefined)
        assert.equal(rebuild.validate({ mode: "entries", connector: "api", origin: "https://api/a" }), undefined)
        assert.match(rebuild.validate({ mode: "everything" }), /Unknown mode/)
        assert.match(rebuild.validate({ mode: "entries", connector: "nope" }), /Unknown connector/)
        assert.match(rebuild.validate({ mode: "entries", connector: "minio" }), /sync/)
        assert.match(rebuild.validate({ mode: "entries", origin: "https://api/a" }), /requires a connector/)
        assert.match(rebuild.validate({ mode: "entries", connector: "api", origin: "" }), /origin/)
        assert.match(rebuild.validate({ mode: "entries", connector: "api", origin: 42 }), /origin/)
    })
})

describe("rebuildEntries", () => {
    test("every origin of the API and Orion collections, refs with their connector; MinIO refs untouched", async () => {
        await insertSources([
            { source: "https://api/a", sourceId: 1, color: "red" },
            { source: "https://api/b", sourceId: 2, color: "blue" }
        ])
        await insertDatapoints([datapoint("https://eurostat/x.xml", ["Lovech", "2020"], 1.5), datapoint("https://eurostat/y.xml", ["Plovdiv", "2020"], 2)])
        // stale entry of origin a that no longer matches its sources, and a MinIO file
        await store.writeAccumulator({ color: { stale: ["public-data"] } }, "https://api/a", {}, "api")
        await store.writeAccumulator({ k: { minio: ["public-data"] } }, "minio://public-data/f.json", {}, "minio")

        const result = await rebuild.rebuildEntries()

        assert.deepEqual(result, { api: { documents: 2 }, orion: { documents: 2 } })
        const colors = await db.docs(Entries, { key: "color" })
        assert.deepEqual(colors.map(e => e.value).sort(), ["blue", "red"])
        const red = colors.find(e => e.value == "red")
        assert.deepEqual([red.refs, red.connectors, red.formats], [[{ origin: "https://api/a", visibility: "public-data", connector: "api", format: "object" }], ["api"], ["object"]])
        const year = (await db.docs(Entries, { key: "dimensions", value: "2020" }))[0]
        assert.deepEqual(year.refs.map(r => [r.origin, r.connector]).sort(), [["https://eurostat/x.xml", "orion"], ["https://eurostat/y.xml", "orion"]])
        assert.deepEqual((await db.docs(Entries, { key: "value" })), []) // orion.datapointsNotIndexed
        assert.deepEqual((await db.docs(Entries, { key: "dupl_hash" })), []) // technical, one value per datapoint
        assert.deepEqual((await db.docs(Key, { key: "value" }))[0].valuesNotIndexed.sort(), ["https://eurostat/x.xml", "https://eurostat/y.xml"])
        assert.equal((await db.docs(Entries, { key: "k" })).length, 1) // MinIO: rebuilt by its sync
    })

    test("one connector only", async () => {
        await insertSources([{ source: "https://api/a", k: "a" }])
        await insertDatapoints([datapoint("https://eurostat/x.xml", ["Lovech"], 1)])
        assert.deepEqual(await rebuild.rebuildEntries({ connector: "orion" }), { orion: { documents: 1 } })
        assert.deepEqual(await db.docs(Entries, { key: "k" }), [])
        assert.equal((await db.docs(Entries, { key: "dimensions" })).length, 1)
    })

    test("deletes legacy documents without refs and legacy refs without connector, unless keepLegacy", async () => {
        await Key.collection.insertOne({ key: "legacy", visibility: ["public-data"] })
        await Values.collection.insertOne({ value: "legacy", visibility: ["public-data"] })
        await store.writeAccumulator({ old: { v: ["public-data"] } }, "minio://b/old.json") // no connector
        await store.writeAccumulator({ old: { v: ["public-data"] } }, "minio://b/new.json", {}, "minio")
        await rebuild.rebuildEntries({ keepLegacy: true })
        assert.equal((await db.docs(Key, { key: "legacy" })).length, 1)
        assert.equal((await db.docs(Entries, { key: "old" }))[0].refs.length, 2)
        await rebuild.rebuildEntries()
        assert.equal((await db.docs(Key, { key: "legacy" })).length, 0)
        assert.equal((await db.docs(Values, { value: "legacy" })).length, 0)
        const [old] = await db.docs(Entries, { key: "old" })
        assert.deepEqual([old.refs, old.connectors], [[{ origin: "minio://b/new.json", visibility: "public-data", connector: "minio" }], ["minio"]])
    })

    test("skips wrapped raw payloads and the mongo _id; documents without origin grouped", async () => {
        await insertSources([
            { source: "https://orion/x", raw: "<xml/>", name: "x" },
            { source: "https://api/a", k: "v" }
        ])
        await insertDatapoints([{ survey: "S", dimensions: ["d"], value: 1, dupl_hash: "no-origin" }])
        await rebuild.rebuildEntries()
        assert.equal((await db.docs(Entries, { key: "raw" })).length, 0)
        assert.equal((await db.docs(Entries, { key: "_id" })).length, 0)
        assert.equal((await db.docs(Entries, { key: "k" })).length, 1)
        assert.equal((await db.docs(Entries, { key: "dimensions" }))[0].refs[0].origin, "orion:no-origin")
    })

    test("with an origin only that origin is rebuilt", async () => {
        await insertSources([{ source: "https://api/a", k: "a" }, { source: "https://api/b", k: "b" }])
        const result = await rebuild.rebuildEntries({ connector: "api", origin: "https://api/b" })
        assert.deepEqual(result, { api: { origins: 1, documents: 1 } })
        assert.deepEqual((await db.docs(Entries, { key: "k" })).map(e => e.value), ["b"])
    })

    test("many origins in one pass: the open collectors are flushed along the way", async () => {
        await insertDatapoints(Array.from({ length: 450 }, (_, i) => datapoint("https://eurostat/" + i + ".xml", ["d" + (i % 3)], i)))
        await rebuild.rebuildEntries({ connector: "orion" })
        const refs = (await db.docs(Entries, { key: "survey" }))[0].refs
        assert.equal(refs.length, 450)
    })

    test("a connector not stored in MongoDB is skipped", async () => {
        config.collections.orion.toMongo = false
        const result = await rebuild.rebuildEntries()
        assert.match(result.orion, /skipped/)
    })
})

describe("mirrorPostgres", () => {
    test("the API table mirrors the API collection; orphans deleted, rows without record.from untouched", async () => {
        await insertSources([
            { source: "https://api/a", name: "a1" },
            { source: "https://api/a", name: "a2" },
            { source: "https://api/b", id: "b1" }
        ])
        await insertPgRow("old-a", { from: "https://api/a" })     // replaced
        await insertPgRow("gone", { from: "https://api/gone" })   // origin no longer in Mongo: deleted
        await insertPgRow("manual", { bucketName: "pilot" })      // no record.from: kept

        const result = await rebuild.mirrorPostgres({ connector: "api" })

        assert.deepEqual(result, { api: { table: "sources", origins: 2, failed: [], orphansDeleted: 1 } })
        const rows = await pgRows()
        assert.deepEqual(rows.map(r => r.name).sort(), ["a1", "a2", "b1", "manual"])
        const a1 = rows.find(r => r.name == "a1")
        assert.deepEqual(a1.record, { from: "https://api/a" })
        assert.deepEqual(a1.data, { source: "https://api/a", name: "a1" }) // no _id
    })

    test("Orion only with collections.orion.toPostgres, in its own table", async () => {
        await insertSources([{ source: "https://api/a", name: "record" }])
        await insertDatapoints([datapoint("https://eurostat/x.xml", ["Lovech"], 1, { name: "dp" })])
        const off = await rebuild.mirrorPostgres()
        assert.match(off.orion, /toPostgres is off/)
        assert.deepEqual(await pgRows("datapoints"), [])
        config.collections.orion.toPostgres = true
        await rebuild.mirrorPostgres({ connector: "orion" })
        assert.deepEqual((await pgRows("datapoints")).map(r => [r.name, r.record.from]), [["dp", "https://eurostat/x.xml"]])
        assert.deepEqual((await pgRows()).map(r => r.name), ["record"]) // the API table is not touched
    })

    test("custom table names", async () => {
        config.collections.api.postgres = "api_log"
        await insertSources([{ source: "https://api/a", name: "x" }])
        await rebuild.mirrorPostgres({ connector: "api" })
        assert.deepEqual((await pgRows("api_log")).map(r => r.name), ["x"])
        await db.query("DROP TABLE api_log")
    })

    test("inserts in batches of 1000 rows", async t => {
        await insertSources(Array.from({ length: 2300 }, (_, i) => ({ source: "https://api/a", name: "n" + i })))
        const query = t.mock.method(Client.prototype, "query")
        await rebuild.mirrorPostgres({ connector: "api" })
        const inserts = query.mock.calls.filter(c => typeof c.arguments[0] === "string" && c.arguments[0].startsWith("INSERT"))
        assert.deepEqual(inserts.map(c => c.arguments[1].length / 3), [1000, 1000, 300])
        assert.deepEqual(await db.query("SELECT count(*)::int AS n FROM sources"), [{ n: 2300 }])
    })

    test("a failing origin is rolled back (its old rows stay) and reported, the others go on", async t => {
        await insertSources([{ source: "https://api/a", name: "a-new" }, { source: "https://api/b", name: "b-new" }])
        await insertPgRow("a-old", { from: "https://api/a" })
        failPgQueries(t, (sql, params) => sql.startsWith("INSERT") && params?.[2]?.from == "https://api/a")

        const result = await rebuild.mirrorPostgres({ connector: "api" })

        assert.deepEqual(result.api.failed, ["https://api/a"])
        assert.deepEqual((await pgRows()).map(r => r.name).sort(), ["a-old", "b-new"])
    })

    test("with an origin: only that origin, no orphan cleanup", async () => {
        await insertSources([{ source: "https://api/a", name: "a" }])
        await insertPgRow("gone", { from: "https://api/gone" })
        const result = await rebuild.mirrorPostgres({ connector: "api", origin: "https://api/a" })
        assert.deepEqual(result, { api: { table: "sources", origins: 1, failed: [], orphansDeleted: 0 } })
        assert.deepEqual((await pgRows()).map(r => r.name).sort(), ["a", "gone"])
    })

    test("the dedicated client is closed even when the first query fails", async t => {
        const end = t.mock.method(Client.prototype, "end")
        failPgQueries(t, sql => sql.startsWith("CREATE TABLE"))
        await assert.rejects(rebuild.mirrorPostgres({ connector: "api" }), /injected failure/)
        assert.equal(end.mock.callCount(), 1)
    })
})

describe("runRebuild", () => {
    test("refuses invalid parameters, a running rebuild and a running poll", async () => {
        await assert.rejects(rebuild.runRebuild({ mode: "nope" }), { code: "INVALID" })
        await assert.rejects(rebuild.runRebuild(), { code: "INVALID", message: /mode is required/ })
        locks.rebuilding = true
        await assert.rejects(rebuild.runRebuild({ mode: "entries" }), { code: "BUSY", message: /rebuild is already running/ })
        locks.rebuilding = false
        locks.polling = true
        await assert.rejects(rebuild.runRebuild({ mode: "entries" }), { code: "BUSY", message: /poll is running/ })
    })

    test("holds the lock while running, releases it and stores the result", async t => {
        await insertSources([{ source: "https://api/a", k: "v" }])
        let lockedDuringRun
        const original = Source.find
        t.mock.method(Source, "find", function (...args) {
            lockedDuringRun = locks.rebuilding
            return original.apply(this, args)
        })
        const result = await rebuild.runRebuild({ mode: "all", connector: "api" })
        assert.equal(lockedDuringRun, true)
        assert.equal(locks.rebuilding, false)
        assert.deepEqual(result.entries, { api: { documents: 1 } })
        assert.equal(result.postgres.api.origins, 1)
        assert.deepEqual((await pgRows()).map(r => r.data.k), ["v"])
        const status = rebuild.getRebuildStatus()
        assert.equal(status.running, false)
        assert.equal(status.mode, "all")
        assert.deepEqual(status.result, result)
    })

    test("postgres mode is skipped when SQLQuery is disabled", async () => {
        config.queryOptions.SQLQuery = false
        const result = await rebuild.runRebuild({ mode: "postgres" })
        assert.match(result.postgres.api, /skipped/)
        assert.match(result.postgres.orion, /skipped/)
        assert.equal(result.entries, undefined)
    })

    test("a failure is stored in the status and the lock is released", async t => {
        failPgQueries(t, sql => sql.startsWith("CREATE TABLE"))
        await assert.rejects(rebuild.runRebuild({ mode: "postgres", connector: "api" }))
        assert.equal(locks.rebuilding, false)
        const status = rebuild.getRebuildStatus()
        assert.equal(status.running, false)
        assert.match(status.error, /injected failure/)
    })
})
