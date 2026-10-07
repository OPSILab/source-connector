// rebuild (entries + PostgreSQL mirror) on real MongoDB + PostgreSQL (see helpers/db.js: npm run test:db).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const { Client } = require("pg")

let rebuild, locks, store, Source, Key, Values, Entries

before(async () => {
    await db.setup(__filename)
    rebuild = load("utils/rebuild.js")
    locks = load("utils/jobLocks.js")
    store = load("utils/entriesStore.js")
    Source = load("api/models/Models.js").Source
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
const minioSource = (name, extra = {}) => ({ name, record: { bucketName: "public-data", s3: { bucket: { name: "public-data" } } }, json: [{ k: "minio" }], ...extra })
const pgRows = () => db.query("SELECT name, data, record FROM sources ORDER BY id")
const insertPgRow = (name, record) => db.query("INSERT INTO sources (name, data, record) VALUES ($1, $2, $3)", [name, {}, record])

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
    test("modes and origin", () => {
        assert.equal(rebuild.validate({}), undefined)
        assert.equal(rebuild.validate({ mode: "entries", origin: "https://api/a" }), undefined)
        assert.match(rebuild.validate({ mode: "everything" }), /Unknown mode/)
        assert.match(rebuild.validate({ origin: "" }), /origin/)
        assert.match(rebuild.validate({ origin: 42 }), /origin/)
    })
})

describe("API_SOURCES filter", () => {
    test("selects API/Orion sources only (string source, no MinIO record)", async () => {
        await insertSources([
            { source: "https://api/a", k: 1 },
            { source: { nested: true } },
            minioSource("f.json", { source: "https://api/a-minio" }),
            { source: "https://api/b", record: { s3: {} } }
        ])
        assert.deepEqual(await Source.distinct("source", rebuild.API_SOURCES), ["https://api/a"])
    })
})

describe("rebuildEntries", () => {
    test("rebuilds the refs of every API origin from the sources collection", async () => {
        await insertSources([
            { source: "https://api/a", sourceId: 1, color: "red" },
            { source: "https://api/b", sourceId: 2, color: "blue" },
            minioSource("public-data/file.json")
        ])
        // stale entry of origin a that no longer matches its sources
        await store.writeAccumulator({ color: { stale: ["public-data"] } }, "https://api/a")

        const result = await rebuild.rebuildEntries()

        assert.deepEqual(result, { origins: 2 })
        const colors = await db.docs(Entries, { key: "color" })
        assert.deepEqual(colors.map(e => e.value).sort(), ["blue", "red"])
        assert.deepEqual(colors.find(e => e.value == "red").refs, [{ origin: "https://api/a", visibility: "public-data" }])
        assert.equal((await db.docs(Entries, { key: "k" })).length, 0) // MinIO sources are not handled here
    })

    test("deletes legacy documents without refs, unless keepLegacy", async () => {
        await Key.collection.insertOne({ key: "legacy", visibility: ["public-data"] })
        await Values.collection.insertOne({ value: "legacy", visibility: ["public-data"] })
        await rebuild.rebuildEntries({ keepLegacy: true })
        assert.equal((await db.docs(Key, { key: "legacy" })).length, 1)
        await rebuild.rebuildEntries()
        assert.equal((await db.docs(Key, { key: "legacy" })).length, 0)
        assert.equal((await db.docs(Values, { value: "legacy" })).length, 0)
    })

    test("skips wrapped raw payloads and the mongo _id", async () => {
        await insertSources([
            { source: "https://orion/x", raw: "<xml/>", name: "x" },
            { source: "https://api/a", k: "v" }
        ])
        await rebuild.rebuildEntries()
        assert.equal((await db.docs(Entries, { key: "raw" })).length, 0)
        assert.equal((await db.docs(Entries, { key: "_id" })).length, 0)
        assert.equal((await db.docs(Entries, { key: "k" })).length, 1)
    })

    test("with an origin only that origin is rebuilt", async () => {
        await insertSources([{ source: "https://api/a", k: "a" }, { source: "https://api/b", k: "b" }])
        const result = await rebuild.rebuildEntries({ origin: "https://api/b" })
        assert.deepEqual(result, { origins: 1 })
        assert.deepEqual((await db.docs(Entries, { key: "k" })).map(e => e.value), ["b"])
    })
})

describe("mirrorPostgres", () => {
    test("the sources table mirrors the API sources; orphans deleted, MinIO rows untouched", async () => {
        await insertSources([
            { source: "https://api/a", name: "a1" },
            { source: "https://api/a", name: "a2" },
            { source: "https://api/b", id: "b1" },
            minioSource("f.json")
        ])
        await insertPgRow("old-a", { from: "https://api/a" })     // replaced
        await insertPgRow("gone", { from: "https://api/gone" })   // origin no longer in Mongo: deleted
        await insertPgRow("minio.json", { bucketName: "pilot" })  // no record.from: not an API row, kept

        const result = await rebuild.mirrorPostgres()

        assert.deepEqual(result, { origins: 2, failed: [], orphansDeleted: 1 })
        const rows = await pgRows()
        assert.deepEqual(rows.map(r => r.name).sort(), ["a1", "a2", "b1", "minio.json"])
        const a1 = rows.find(r => r.name == "a1")
        assert.deepEqual(a1.record, { from: "https://api/a" })
        assert.deepEqual(a1.data, { source: "https://api/a", name: "a1" }) // no _id
    })

    test("inserts in batches of 1000 rows", async t => {
        await insertSources(Array.from({ length: 2300 }, (_, i) => ({ source: "https://api/a", name: "n" + i })))
        const query = t.mock.method(Client.prototype, "query")
        await rebuild.mirrorPostgres()
        const inserts = query.mock.calls.filter(c => typeof c.arguments[0] === "string" && c.arguments[0].startsWith("INSERT"))
        assert.deepEqual(inserts.map(c => c.arguments[1].length / 3), [1000, 1000, 300])
        assert.deepEqual(await db.query("SELECT count(*)::int AS n FROM sources"), [{ n: 2300 }])
    })

    test("a failing origin is rolled back (its old rows stay) and reported, the others go on", async t => {
        await insertSources([{ source: "https://api/a", name: "a-new" }, { source: "https://api/b", name: "b-new" }])
        await insertPgRow("a-old", { from: "https://api/a" })
        failPgQueries(t, (sql, params) => sql.startsWith("INSERT") && params?.[2]?.from == "https://api/a")

        const result = await rebuild.mirrorPostgres()

        assert.deepEqual(result.failed, ["https://api/a"])
        assert.deepEqual((await pgRows()).map(r => r.name).sort(), ["a-old", "b-new"])
    })

    test("with an origin: only that origin, no orphan cleanup", async () => {
        await insertSources([{ source: "https://api/a", name: "a" }])
        await insertPgRow("gone", { from: "https://api/gone" })
        const result = await rebuild.mirrorPostgres({ origin: "https://api/a" })
        assert.deepEqual(result, { origins: 1, failed: [], orphansDeleted: 0 })
        assert.deepEqual((await pgRows()).map(r => r.name).sort(), ["a", "gone"])
    })

    test("the dedicated client is closed even when the first query fails", async t => {
        const end = t.mock.method(Client.prototype, "end")
        failPgQueries(t, sql => sql.startsWith("CREATE TABLE"))
        await assert.rejects(rebuild.mirrorPostgres(), /injected failure/)
        assert.equal(end.mock.callCount(), 1)
    })
})

describe("runRebuild", () => {
    test("refuses invalid parameters, a running rebuild and a running poll", async () => {
        await assert.rejects(rebuild.runRebuild({ mode: "nope" }), { code: "INVALID" })
        locks.rebuilding = true
        await assert.rejects(rebuild.runRebuild(), { code: "BUSY", message: /rebuild is already running/ })
        locks.rebuilding = false
        locks.polling = true
        await assert.rejects(rebuild.runRebuild(), { code: "BUSY", message: /poll is running/ })
    })

    test("holds the lock while running, releases it and stores the result", async t => {
        await insertSources([{ source: "https://api/a", k: "v" }])
        let lockedDuringRun
        const original = Source.find
        t.mock.method(Source, "find", function (...args) {
            lockedDuringRun = locks.rebuilding
            return original.apply(this, args)
        })
        const result = await rebuild.runRebuild({ mode: "all" })
        assert.equal(lockedDuringRun, true)
        assert.equal(locks.rebuilding, false)
        assert.deepEqual(result.entries, { origins: 1 })
        assert.equal(result.postgres.origins, 1)
        assert.deepEqual((await pgRows()).map(r => r.data.k), ["v"])
        const status = rebuild.getRebuildStatus()
        assert.equal(status.running, false)
        assert.equal(status.mode, "all")
        assert.deepEqual(status.result, result)
    })

    test("postgres mode is skipped when SQLQuery is disabled", async () => {
        config.queryOptions.SQLQuery = false
        const result = await rebuild.runRebuild({ mode: "postgres" })
        assert.match(result.postgres, /skipped/)
        assert.equal(result.entries, undefined)
    })

    test("a failure is stored in the status and the lock is released", async t => {
        failPgQueries(t, sql => sql.startsWith("CREATE TABLE"))
        await assert.rejects(rebuild.runRebuild({ mode: "postgres" }))
        assert.equal(locks.rebuilding, false)
        const status = rebuild.getRebuildStatus()
        assert.equal(status.running, false)
        assert.match(status.error, /injected failure/)
    })
})
