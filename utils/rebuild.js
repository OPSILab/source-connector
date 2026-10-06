// Rebuild from the Mongo `sources` collection (API / Orion records, i.e. docs with `source: <url>`).
// Used by the HTTP endpoints (POST/GET /rebuild) and by scripts/rebuildFromSources.js.
//
//   entries   rebuilds keys/values/entries refs of every API origin from its sources
//             (+ deletes legacy keys/values/entries without refs, unless keepLegacy)
//   postgres  makes the PostgreSQL `sources` table a mirror of the API sources in Mongo
//             (rows of origins no longer in Mongo are deleted too, unless a single origin is given)
//   all       both
//
// MinIO files are not handled here: minioConnector.sync() already rebuilds them from MinIO.

const config = require("../config")
const logger = require("percocologger")
const { Client } = require("pg")
const Source = require("../api/models/Models").Source
const Key = require("../api/models/Key")
const Values = require("../api/models/Value")
const Entries = require("../api/models/Entries")
const { createCollector, removeOrigin } = require("./entriesStore")
const locks = require("./jobLocks")

const MODES = ["entries", "postgres", "all"]
// API/Orion sources: `source` is the origin url; MinIO sources always have record.bucketName or record.s3
const API_SOURCES = { source: { $type: "string" }, "record.bucketName": { $exists: false }, "record.s3": { $exists: false } }
const PG_BATCH_SIZE = 1000 // rows per INSERT (3 params per row, far below the 65535 limit)

let status = { running: false }

// Same rule as sourceRecords.replaceRecords: non-object payloads (e.g. XML) are stored as { raw } and not indexed
function isWrappedRaw(doc) {
    return doc.raw !== undefined && (doc.raw === null || typeof doc.raw !== "object")
}

async function getOrigins(origin) {
    return origin ? [origin] : await Source.distinct("source", API_SOURCES)
}

async function rebuildEntries({ origin, keepLegacy } = {}) {
    if (!keepLegacy)
        for (const Model of [Key, Values, Entries]) {
            const { deletedCount } = await Model.collection.deleteMany({ refs: { $exists: false } })
            logger.info(`${Model.modelName}: ${deletedCount} legacy docs without refs deleted`)
        }
    const origins = await getOrigins(origin)
    let index = 0
    for (const o of origins) {
        logger.info(`Entries ${++index}/${origins.length}: ${o}`)
        await removeOrigin(o)
        const collector = createCollector(o)
        let count = 0
        for await (const doc of Source.find({ ...API_SOURCES, source: o }).lean().cursor())
            if (!isWrappedRaw(doc)) {
                await collector.add([doc]) // the collector drops _id and flushes every N items
                count++
            }
        await collector.flush()
        logger.info(`Entries rebuilt for ${o} from ${count} sources`)
    }
    return { origins: origins.length }
}

// Dedicated connection: the service's shared pg client is also used by minioConnector, and a
// transaction on it would include (and on rollback, undo) its concurrent writes.
async function mirrorPostgres({ origin } = {}) {
    const pg = new Client(config.postgreConfig)
    await pg.connect()
    const failed = []
    try {
        await pg.query(`CREATE TABLE IF NOT EXISTS sources (id SERIAL PRIMARY KEY, name TEXT, data JSONB, record JSONB)`)

        async function insertBatch(rows, o) {
            if (!rows.length)
                return
            const values = []
            const placeholders = rows.map((row, i) => {
                const { _id, ...data } = row
                values.push(data.name || data.id || o, data, { from: o })
                return `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`
            })
            await pg.query(`INSERT INTO sources (name, data, record) VALUES ${placeholders.join(", ")}`, values)
        }

        const origins = await getOrigins(origin)
        let index = 0
        for (const o of origins) {
            logger.info(`Postgres ${++index}/${origins.length}: ${o}`)
            // one transaction per origin: readers never see the origin half-empty
            await pg.query("BEGIN")
            try {
                await pg.query(`DELETE FROM sources WHERE record->>'from' = $1`, [o])
                let batch = []
                let count = 0
                for await (const doc of Source.find({ ...API_SOURCES, source: o }).lean().cursor()) {
                    batch.push(doc)
                    count++
                    if (batch.length >= PG_BATCH_SIZE) {
                        await insertBatch(batch, o)
                        batch = []
                    }
                }
                await insertBatch(batch, o)
                await pg.query("COMMIT")
                logger.info(`Postgres mirrored for ${o}: ${count} rows`)
            }
            catch (error) {
                await pg.query("ROLLBACK")
                failed.push(o)
                logger.error(`Postgres mirror failed for ${o}, rolled back`, error)
            }
        }

        let orphansDeleted = 0
        if (!origin) {
            // rows of origins that no longer exist in Mongo
            const { rowCount } = await pg.query(
                `DELETE FROM sources WHERE record->>'from' IS NOT NULL AND NOT (record->>'from' = ANY($1::text[]))`,
                [origins]
            )
            orphansDeleted = rowCount
            logger.info(`Postgres: ${rowCount} rows of origins no longer in Mongo deleted`)
        }
        return { origins: origins.length, failed, orphansDeleted }
    }
    finally {
        await pg.end().catch(() => { })
    }
}

function validate({ mode = "all", origin } = {}) {
    if (!MODES.includes(mode))
        return `Unknown mode "${mode}" (${MODES.join(" | ")})`
    if (origin !== undefined && (typeof origin != "string" || !origin))
        return "origin must be a non-empty string"
}

// Runs a rebuild holding the shared lock. Throws (with .code) if one is already running or a poll is in progress.
async function runRebuild({ mode = "all", origin, keepLegacy = false } = {}) {
    const invalid = validate({ mode, origin })
    if (invalid)
        throw Object.assign(new Error(invalid), { code: "INVALID" })
    if (locks.rebuilding)
        throw Object.assign(new Error("A rebuild is already running"), { code: "BUSY" })
    if (locks.polling)
        throw Object.assign(new Error("An API poll is running, retry when it has finished"), { code: "BUSY" })

    locks.rebuilding = true
    status = { running: true, mode, origin: origin || null, keepLegacy, startedAt: new Date() }
    try {
        const result = {}
        if (mode == "entries" || mode == "all")
            result.entries = await rebuildEntries({ origin, keepLegacy })
        if (mode == "postgres" || mode == "all") {
            if (config.queryOptions.SQLQuery)
                result.postgres = await mirrorPostgres({ origin })
            else {
                result.postgres = "skipped: queryOptions.SQLQuery is disabled"
                logger.warn("queryOptions.SQLQuery is disabled: PostgreSQL mirror skipped")
            }
        }
        status = { ...status, running: false, finishedAt: new Date(), result }
        logger.info("Rebuild finished", result)
        return result
    }
    catch (error) {
        status = { ...status, running: false, finishedAt: new Date(), error: error.message || String(error) }
        logger.error("Rebuild failed", error)
        throw error
    }
    finally {
        locks.rebuilding = false
    }
}

module.exports = {
    MODES,
    API_SOURCES,
    rebuildEntries,
    mirrorPostgres,
    validate,
    runRebuild,
    getRebuildStatus: () => status
}
