// Backups of the apiConnector data (utils/apiBackups.js) and their endpoints, on real MongoDB + PostgreSQL (see
// helpers/db.js): backup into a database of their own, restore (after a backup of the data as they are), delete,
// maxBackups, pending polls, records in PostgreSQL only.
const { load, stub, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

const A = "https://api.example/a"
const B = "https://api.example/b"
// the router requires service.js, which starts the MinIO / Orion connectors, and apiConnector, which starts polling
stub("api/services/service.js", {})
stub("inputConnectors/apiConnector.js", { pollAPI: async () => { } })

let records, Source, Entries, IncrementalState, Status, pollStatus, locks, backups, mongoose, server, baseUrl

before(async () => {
    await db.setup(__filename)
    records = load("utils/sourceRecords.js")
    Source = load("utils/collections.js").collectionModel("api")
    Entries = load("api/models/Entries.js")
    IncrementalState = load("api/models/IncrementalState.js")
    Status = load("api/models/Status.js")
    pollStatus = load("utils/pollStatus.js")
    locks = load("utils/jobLocks.js")
    backups = load("utils/apiBackups.js")
    mongoose = require("mongoose")
    await pollStatus.ensurePgStatusTable()
    const app = express()
    app.use(express.json())
    app.use("/api", load("api/routes/router.js"))
    await new Promise(resolve => { server = app.listen(0, "127.0.0.1", resolve) })
    baseUrl = `http://127.0.0.1:${server.address().port}/api`
})
after(async () => {
    server?.close()
    await backups.backupDb().dropDatabase().catch(() => { })
    await db.teardown()
})

const backupDb = () => backups.backupDb()
async function store(url, items) {
    const { createCollector } = load("utils/entriesStore.js")
    const collector = createCollector(url, "api")
    const stored = await records.storeRecords(items.map(item => records.makeItem({ ...item }, url)), url, { collector })
    await collector.flush()
    return stored
}
const values = async () => (await Source.find({}).sort({ _id: 1 }).lean()).map(doc => doc.v).sort()
const pgValues = async () => (await db.query("SELECT data->>'v' AS v FROM sources ORDER BY 1")).map(row => row.v)
const entryValues = async () => (await Entries.collection.find({ key: "v" }).toArray()).map(doc => doc.value).sort()

beforeEach(async () => {
    await db.clean()
    await db.query("TRUNCATE status RESTART IDENTITY")
    await backupDb().dropDatabase()
    locks.polling = locks.rebuilding = false
    config.managementSecret = "s3cret"
    config.authConfig.disableAuth = true
    delete config.apiBackup
    config.collections.api.toMongo = true
    config.collections.api.toPostgres = true
    await store(A, [{ id: 1, v: "a1" }, { id: 2, v: "a2" }])
    await store(B, [{ id: 3, v: "b1" }])
    await IncrementalState.collection.insertOne({ _id: A, lastEnd: new Date("2026-11-10T00:00:00Z"), complete: true })
})

describe("apiBackups", () => {
    test("a backup: in the database of the backups (the main one untouched), listed with its counts", async () => {
        const { id, counts } = await backups.createBackup({ label: "before the change", by: "anna" })
        assert.match(id, backups.VALID_ID)
        assert.deepEqual(counts, { records: 3, incrementalstate: 1 })
        assert.equal(backupDb().databaseName, mongoose.connection.db.databaseName + "_backups")
        assert.equal(await backupDb().collection(`${id}.records`).countDocuments(), 3)
        const main = (await mongoose.connection.db.listCollections({}, { nameOnly: true }).toArray()).map(c => c.name)
        assert.ok(main.every(name => !name.startsWith("bk_")), main.join())
        const [listed] = await backups.listBackups()
        assert.deepEqual([listed.id, listed.label, listed.by, listed.kind, listed.counts], [id, "before the change", "anna", "manual", counts])
    })

    test("restore: the data as they are backed up first, then the backup in their place; entries and PostgreSQL rebuilt", async () => {
        const { id } = await backups.createBackup({ by: "anna" })
        // after the backup: new records, another state
        await store(A, [{ id: 4, v: "new" }])
        await IncrementalState.collection.updateOne({ _id: A }, { $set: { lastEnd: new Date("2026-11-17T00:00:00Z") } })
        assert.deepEqual(await values(), ["a1", "a2", "b1", "new"])

        const result = await backups.restoreBackup({ id, by: "bob" })
        assert.deepEqual(await values(), ["a1", "a2", "b1"])
        assert.deepEqual(await pgValues(), ["a1", "a2", "b1"])
        assert.deepEqual(await entryValues(), ["a1", "a2", "b1"])
        assert.equal((await IncrementalState.collection.findOne({ _id: A })).lastEnd.toISOString(), "2026-11-10T00:00:00.000Z")
        // the data before the restore: in a backup of their own, which restores them back
        const list = await backups.listBackups()
        const beforeRestore = list.find(item => item.id === result.before)
        assert.deepEqual([beforeRestore.kind, beforeRestore.counts.records], ["before-restore", 4])
        assert.equal(list.find(item => item.id === id).restoredBy, "bob")
        await backups.restoreBackup({ id: result.before, by: "bob" })
        assert.deepEqual(await values(), ["a1", "a2", "b1", "new"])
        assert.deepEqual(await pgValues(), ["a1", "a2", "b1", "new"])
    })

    test("the records of an interrupted poll are not backed up (dropped first, as the next poll would do)", async () => {
        const stored = await store(A, [{ id: 5, v: "half" }])
        await pollStatus.markPending(A, new Date(), stored)
        const { counts } = await backups.createBackup({})
        assert.equal(counts.records, 3)
        assert.deepEqual(await values(), ["a1", "a2", "b1"])
        assert.equal(await Status.collection.countDocuments({ pending: true }), 0)
    })

    test("maxBackups: the oldest beyond it deleted, never the one being restored", async () => {
        config.apiBackup = { maxBackups: 2 }
        const first = (await backups.createBackup({})).id
        const second = (await backups.createBackup({})).id
        const third = await backups.createBackup({})
        assert.deepEqual(third.pruned, [first])
        assert.deepEqual((await backups.listBackups()).map(item => item.id), [third.id, second])
        assert.equal(await backupDb().collection(`${first}.records`).countDocuments(), 0)
        // restoring the oldest: its before-restore backup would push it out, it is kept
        await backups.restoreBackup({ id: second })
        const ids = (await backups.listBackups()).map(item => item.id)
        assert.ok(ids.includes(second) && ids.length == 2, ids.join())
    })

    test("refused while a poll or another job runs; an unknown backup: not found; delete", async () => {
        locks.polling = true
        await assert.rejects(backups.createBackup({}), error => error.code == "BUSY")
        locks.polling = false
        await assert.rejects(backups.restoreBackup({ id: "bk_20260101T000000000Z" }), error => error.code == "NOT_FOUND")
        await assert.rejects(backups.restoreBackup({ id: "../sources" }), error => error.code == "NOT_FOUND")
        const { id } = await backups.createBackup({})
        await backups.deleteBackup({ id, by: "anna" })
        assert.deepEqual(await backups.listBackups(), [])
        assert.equal((await backupDb().listCollections({ name: `${id}.records` }).toArray()).length, 0)
        const log = await backups.listLog()
        assert.deepEqual(log.map(entry => [entry.action, entry.outcome]).slice(0, 2), [["delete", "done"], ["backup", "done"]])
    })

    test("apiBackup.mongoDb: another database; the main one refused", async () => {
        config.apiBackup = { mongoDb: mongoose.connection.db.databaseName }
        await assert.rejects(backups.createBackup({}), error => error.code == "INVALID")
        config.apiBackup = { mongoDb: mongoose.connection.db.databaseName + "_other" }
        const { id } = await backups.createBackup({})
        assert.equal(await mongoose.connection.client.db(config.apiBackup.mongoDb).collection(`${id}.records`).countDocuments(), 3)
        await mongoose.connection.client.db(config.apiBackup.mongoDb).dropDatabase()
    })

    test("records in PostgreSQL only (toMongo off): its rows backed up and restored, the ids going on after", async () => {
        config.collections.api.toMongo = false
        const { id, counts } = await backups.createBackup({})
        assert.deepEqual(counts, { incrementalstate: 1, postgres: 3 })
        await store(A, [{ id: 6, v: "pg-new" }])
        await backups.restoreBackup({ id })
        assert.deepEqual(await pgValues(), ["a1", "a2", "b1"])
        await store(B, [{ id: 7, v: "after" }]) // the serial goes on: no duplicated id
        assert.deepEqual(await pgValues(), ["a1", "a2", "after", "b1"])
    })
})

describe("endpoints", () => {
    const call = (method, path, { secret, body } = {}) => fetch(baseUrl + path, {
        method, headers: { "Content-Type": "application/json", ...(secret ? { "X-Management-Secret": secret } : {}) }, ...(body ? { body: JSON.stringify(body) } : {})
    })
    const settle = async () => { while (backups.getBackupStatus().running) await new Promise(resolve => setTimeout(resolve, 20)) }

    test("list for the logged users; backup / restore / delete with the management password", async () => {
        assert.equal((await call("POST", "/backups")).status, 403)
        assert.equal((await call("POST", "/backups", { secret: "wrong" })).status, 403)
        let res = await call("POST", "/backups", { secret: "s3cret", body: { label: "  weekly " } })
        assert.deepEqual([res.status, await res.json()], [202, { started: true }])
        await settle()
        res = await call("GET", "/backups")
        const list = await res.json()
        assert.deepEqual([list.items.length, list.items[0].label, list.maxBackups, list.secretConfigured, list.status.action], [1, "weekly", 10, true, "backup"])
        const id = list.items[0].id
        assert.equal((await call("POST", "/backups/bk_20260101T000000000Z/restore", { secret: "s3cret" })).status, 404)
        locks.polling = true
        assert.equal((await call("POST", `/backups/${id}/restore`, { secret: "s3cret" })).status, 409)
        locks.polling = false
        assert.equal((await call("POST", `/backups/${id}/restore`, { secret: "s3cret" })).status, 202)
        await settle()
        assert.equal((await (await call("GET", "/backups")).json()).items.length, 2) // + the data before the restore
        assert.equal((await call("DELETE", `/backups/${id}`)).status, 403)
        assert.equal((await call("DELETE", `/backups/${id}`, { secret: "s3cret" })).status, 200)
        const log = (await (await call("GET", "/backups/log?limit=10")).json()).items
        assert.deepEqual(log.map(entry => entry.action), ["delete", "restore", "backup"])
        config.managementSecret = ""
        assert.equal((await call("POST", "/backups", { secret: "s3cret" })).status, 403)
    })
})
