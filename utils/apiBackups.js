// Backups of the apiConnector data, in a MongoDB database of their own (config apiBackup.mongoDb, default
// "<main database>_backups"): the main database stays clean, and the Query-Engine never sees the backups (it does not
// know that database; nothing is copied into PostgreSQL, whose SQL the Query-Engine runs).
//
// A backup (id "bk_<UTC date and time>") copies, by the MongoDB server ($out, MongoDB 4.4 or later):
//   - the records of the API collection (collections.api.mongo), if collections.api.toMongo
//   - the state of the incremental polls (incrementalstate)
//   - the rows of the API table (collections.api.postgres), only if the records are in PostgreSQL alone (toMongo off)
// into "<id>.records", "<id>.incrementalstate", "<id>.postgres", and is listed in "apibackups" (date, who, label,
// counts, last restore). Before copying, the records of interrupted polls are dropped (utils/pollStatus.js, as the
// next poll would do): a backup never holds half a window.
//
// Restore = a backup of the data as they are now (kind "before-restore"), then the backup replaces them ($out keeps
// the indexes of the collection), the pending poll marks are cleared, and keys / values / entries of the API records
// are rebuilt (utils/rebuild.js), with the PostgreSQL mirror of the records (toMongo and toPostgres). The polls then go
// on from the state of the backup: the incremental APIs ask again what came after it.
//
// apiBackup.maxBackups (default 10; 0: no limit): after a backup, the oldest beyond it are deleted (never the one being
// restored). Every action (backup, restore, delete) is logged in "apibackuplog". The shared lock (utils/jobLocks.js):
// refused while a poll, a rebuild or a reset runs; while it runs, the scheduled polls are skipped.
const config = require("../config")
const logger = require("percocologger")
const mongoose = require("mongoose")
const { Client } = require("pg")
const locks = require("./jobLocks")
const pollStatus = require("./pollStatus")
const Status = require("../api/models/Status")
const IncrementalState = require("../api/models/IncrementalState")
const rebuild = require("./rebuild")
const { collectionSettings } = require("./collections")
const { pgQuery, waitForPostgreInit, ensurePgTable } = require("./sourceRecords")

const REGISTRY = "apibackups"
const LOG = "apibackuplog"
const PG_BATCH = 1000
const DEFAULT_MAX = 10

let status = { running: false }

const mainDb = () => mongoose.connection.db

// The database of the backups (never the main one)
function backupDb() {
    const main = mainDb().databaseName
    const name = config.apiBackup?.mongoDb || main + "_backups"
    if (name === main)
        throw Object.assign(new Error("apiBackup.mongoDb must be another database than the main one"), { code: "INVALID" })
    return mongoose.connection.client.db(name)
}

const maxBackups = () => {
    const max = Number(config.apiBackup?.maxBackups ?? DEFAULT_MAX)
    return Number.isInteger(max) && max >= 0 ? max : DEFAULT_MAX
}

const newId = (date = new Date()) => "bk_" + date.toISOString().replace(/[-:]/g, "").replace(".", "")
const VALID_ID = /^bk_\d{8}T\d{9}Z$/

async function exists(db, name) {
    return (await db.listCollections({ name }, { nameOnly: true }).toArray()).length > 0
}

// A whole collection copied into another database by the server; the target is replaced (its indexes kept)
async function copyCollection(fromDb, from, toDb, to) {
    if (!(await exists(fromDb, from))) {
        // nothing to copy: the target emptied (created if missing)
        if (await exists(toDb, to))
            await toDb.collection(to).deleteMany({})
        else
            await toDb.createCollection(to)
        return 0
    }
    await fromDb.collection(from).aggregate([{ $match: {} }, { $out: { db: toDb.databaseName, coll: to } }]).toArray()
    return toDb.collection(to).countDocuments()
}

async function pgClient() {
    const client = new Client(config.postgreConfig)
    await client.connect()
    return client
}

// The urls with records of an interrupted poll (MongoDB / PostgreSQL marks)
async function pendingUrls() {
    const settings = collectionSettings("api")
    const urls = new Set(settings.toMongo ? await Status.collection.distinct("url", { pending: true }) : [])
    if (settings.toPostgres) {
        await waitForPostgreInit()
        await pollStatus.ensurePgStatusTable()
        for (const row of (await pgQuery(`SELECT DISTINCT url FROM ${pollStatus.PG_TABLE} WHERE pending`)).rows)
            urls.add(row.url)
    }
    return [...urls]
}

// The rows of the API table copied into the backup database, in batches
async function copyPgRows(table, target) {
    await waitForPostgreInit()
    await ensurePgTable(table)
    const collection = backupDb().collection(target)
    let last = -1, count = 0
    for (;;) {
        const rows = (await pgQuery(`SELECT * FROM ${table} WHERE id > $1 ORDER BY id LIMIT ${PG_BATCH}`, [last])).rows
        if (!rows.length)
            break
        await collection.insertMany(rows.map(row => ({ ...row })))
        last = rows[rows.length - 1].id
        count += rows.length
    }
    if (!count)
        await backupDb().createCollection(target).catch(() => { })
    return count
}

// The API table replaced by the rows of a backup, in one transaction (readers never see it empty)
async function restorePgRows(table, source) {
    await waitForPostgreInit()
    await ensurePgTable(table)
    const pg = await pgClient()
    try {
        await pg.query("BEGIN")
        await pg.query(`TRUNCATE ${table}`)
        let batch = []
        const flush = async () => {
            if (!batch.length)
                return
            const columns = Object.keys(batch[0])
            const values = []
            const placeholders = batch.map((row, r) => "(" + columns.map((column, c) => {
                values.push(row[column])
                return `$${r * columns.length + c + 1}`
            }).join(", ") + ")")
            await pg.query(`INSERT INTO ${table} (${columns.join(", ")}) VALUES ${placeholders.join(", ")}`, values)
            batch = []
        }
        for await (const { _id, ...row } of backupDb().collection(source).find({}).sort({ id: 1 })) {
            batch.push(row)
            if (batch.length >= PG_BATCH)
                await flush()
        }
        await flush()
        await pg.query(`SELECT setval(pg_get_serial_sequence('${table}', 'id'), COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM ${table}`)
        await pg.query("COMMIT")
    }
    catch (error) {
        await pg.query("ROLLBACK").catch(() => { })
        throw error
    }
    finally {
        await pg.end().catch(() => { })
    }
}

async function dropBackupCollections(id) {
    const db = backupDb()
    for (const suffix of ["records", "incrementalstate", "postgres"])
        if (await exists(db, `${id}.${suffix}`))
            await db.collection(`${id}.${suffix}`).drop()
}

// The oldest backups beyond apiBackup.maxBackups deleted (keep: ids never deleted, counted in the limit)
async function prune(keep = []) {
    const max = maxBackups()
    if (!max)
        return []
    const registry = backupDb().collection(REGISTRY)
    const others = (await registry.find({ _id: { $nin: keep } }, { projection: { _id: 1 } }).sort({ createdAt: -1, _id: -1 }).toArray())
        .map(entry => entry._id)
    const removed = []
    for (const _id of others.slice(Math.max(0, max - keep.length))) {
        await dropBackupCollections(_id)
        await registry.deleteOne({ _id })
        removed.push(_id)
    }
    if (removed.length)
        logger.info(`API backups beyond apiBackup.maxBackups (${max}) deleted:`, removed)
    return removed
}

// A backup, the lock already held
async function makeBackup({ label, by, kind = "manual", keep = [] }) {
    const settings = collectionSettings("api")
    let date = new Date()
    while (await backupDb().collection(REGISTRY).findOne({ _id: newId(date) })) // two backups in the same millisecond
        date = new Date(date.getTime() + 1)
    const id = newId(date)
    for (const url of await pendingUrls())
        await pollStatus.rollbackPending(url, { logName: url })
    const counts = {}
    if (settings.toMongo)
        counts.records = await copyCollection(mainDb(), settings.mongo, backupDb(), `${id}.records`)
    counts.incrementalstate = await copyCollection(mainDb(), IncrementalState.collection.collectionName, backupDb(), `${id}.incrementalstate`)
    if (!settings.toMongo && settings.toPostgres)
        counts.postgres = await copyPgRows(settings.postgres, `${id}.postgres`)
    const entry = {
        _id: id, createdAt: new Date(), by: by || "unknown", kind, ...(label ? { label: String(label).slice(0, 200) } : {}),
        counts, collections: { mongo: settings.toMongo ? settings.mongo : null, postgres: settings.toPostgres ? settings.postgres : null }
    }
    await backupDb().collection(REGISTRY).insertOne(entry)
    const pruned = await prune([id, ...keep])
    logger.info(`API backup ${id} made (${kind})`, counts)
    return { id, counts, ...(pruned.length ? { pruned } : {}) }
}

// The data replaced by a backup, the lock already held
async function applyBackup(entry) {
    const settings = collectionSettings("api")
    const id = entry._id
    const db = backupDb()
    const result = {}
    if (settings.toMongo) {
        if (!(await exists(db, `${id}.records`)) && entry.counts?.postgres !== undefined)
            throw Object.assign(new Error(`Backup ${id} holds the records of PostgreSQL only (collections.api.toMongo was off): it cannot fill MongoDB`), { code: "INVALID" })
        result.records = await copyCollection(db, `${id}.records`, mainDb(), settings.mongo)
    }
    result.incrementalstate = await copyCollection(db, `${id}.incrementalstate`, mainDb(), IncrementalState.collection.collectionName)
    // the pending marks refer to records no longer there
    await Status.collection.deleteMany({ pending: true })
    if (settings.toPostgres) {
        await waitForPostgreInit()
        await pollStatus.ensurePgStatusTable()
        await pgQuery(`DELETE FROM ${pollStatus.PG_TABLE}`)
    }
    if (settings.toMongo) {
        result.entries = await rebuild.rebuildEntries({ connector: "api", keepLegacy: true })
        if (settings.toPostgres)
            result.postgres = (await rebuild.mirrorPostgres({ connector: "api" })).api
    }
    else if (settings.toPostgres) {
        if (!(await exists(db, `${id}.postgres`)))
            throw Object.assign(new Error(`Backup ${id} has no PostgreSQL rows (collections.api.toMongo was on): the table is rebuilt from MongoDB, now off`), { code: "INVALID" })
        await restorePgRows(settings.postgres, `${id}.postgres`)
        result.postgres = Number((await pgQuery(`SELECT count(*) AS n FROM ${settings.postgres}`)).rows[0].n)
    }
    return result
}

async function writeLog(fields) {
    try {
        await backupDb().collection(LOG).insertOne({ at: new Date(), ...fields })
    }
    catch (error) {
        logger.error("apibackuplog not written:", error?.message || error)
    }
}

// Runs an action holding the shared lock, with its status and its log. The lock is released before the status says
// "not running": who sees it finished can start the next action at once.
async function run(action, fields, work) {
    if (locks.rebuilding)
        throw Object.assign(new Error("A rebuild, a reset or a backup is running"), { code: "BUSY" })
    if (locks.polling)
        throw Object.assign(new Error("An API poll is running, retry when it has finished"), { code: "BUSY" })
    locks.rebuilding = true
    status = { running: true, action, ...fields, startedAt: new Date() }
    let result, failure
    try {
        result = await work()
    }
    catch (error) {
        failure = error
    }
    finally {
        locks.rebuilding = false
    }
    if (failure) {
        const message = failure.message || String(failure)
        await writeLog({ action, ...fields, outcome: "error", error: message })
        status = { ...status, running: false, finishedAt: new Date(), error: message }
        logger.error(`API backup ${action} failed`, failure)
        throw failure
    }
    await writeLog({ action, ...fields, outcome: "done", result })
    status = { ...status, running: false, finishedAt: new Date(), result }
    return result
}

async function findBackup(id) {
    const entry = VALID_ID.test(String(id)) ? await backupDb().collection(REGISTRY).findOne({ _id: id }) : null
    if (!entry)
        throw Object.assign(new Error(`No API backup ${id}`), { code: "NOT_FOUND" })
    return entry
}

// A backup of the data as they are now
async function createBackup({ label, by } = {}) {
    backupDb()
    return run("backup", { by: by || "unknown", ...(label ? { label } : {}) }, () => makeBackup({ label, by }))
}

// The data replaced by backup `id`, after a backup of them
// (the lock and the status taken at once, before anything is awaited: who asks the status right after sees it running)
async function restoreBackup({ id, by } = {}) {
    return run("restore", { by: by || "unknown", id }, async () => {
        const entry = await findBackup(id)
        const before = await makeBackup({ label: `before restoring ${id}`, by, kind: "before-restore", keep: [id] })
        status = { ...status, phase: "restoring", before: before.id }
        const restored = await applyBackup(entry)
        await backupDb().collection(REGISTRY).updateOne({ _id: id }, { $set: { restoredAt: new Date(), restoredBy: by || "unknown" } })
        logger.info(`API backup ${id} restored (the data before it: backup ${before.id})`, restored)
        return { id, before: before.id, restored }
    })
}

async function deleteBackup({ id, by } = {}) {
    await findBackup(id)
    return run("delete", { by: by || "unknown", id }, async () => {
        await dropBackupCollections(id)
        await backupDb().collection(REGISTRY).deleteOne({ _id: id })
        return { id }
    })
}

// The backups, newest first
async function listBackups() {
    return (await backupDb().collection(REGISTRY).find({}).sort({ createdAt: -1, _id: -1 }).toArray())
        .map(({ _id, ...entry }) => ({ id: _id, ...entry }))
}

async function listLog({ limit = 100 } = {}) {
    return (await backupDb().collection(LOG).find({}).sort({ at: -1, _id: -1 }).limit(limit).toArray())
        .map(({ _id, ...entry }) => ({ ...entry, logId: String(_id) }))
}

module.exports = {
    createBackup, restoreBackup, deleteBackup, listBackups, listLog, maxBackups, backupDb, VALID_ID,
    getBackupStatus: () => status
}
