// Rebuild from the MongoDB collections of the API and Orion connectors (utils/collections.js).
// Used by the HTTP endpoints (POST/GET /rebuild) and by scripts/rebuildFromSources.js.
//
//   entries   rebuilds the keys/values/entries refs of the connector(s) from their collection; also removes the
//             legacy keys/values/entries (no refs, or refs without connector) unless keepLegacy
//   postgres  makes the connector's PostgreSQL table a mirror of its MongoDB collection (rows of origins no longer
//             in MongoDB are deleted too, unless a single origin is given); only for toPostgres connectors
//   all       both
// connector: api | orion (default: both). The mode is always explicit (no default).
//
// MinIO files are not handled here: minioConnector.sync() rebuilds them from MinIO.

const config = require("../config")
const logger = require("percocologger")
const { Client } = require("pg")
const Key = require("../api/models/Key")
const Values = require("../api/models/Value")
const Entries = require("../api/models/Entries")
const { createCollector, removeOrigin, removeRefs } = require("./entriesStore")
const { collectionSettings, collectionModel } = require("./collections")
const { ensureOriginIndex } = require("./sourceRecords")
const locks = require("./jobLocks")

const MODES = ["entries", "postgres", "all"]
const REBUILT_CONNECTORS = ["api", "orion"]
const PG_BATCH_SIZE = 1000   // rows per INSERT (3 params per row, far below the 65535 limit)
const MAX_OPEN_COLLECTORS = 200 // single-pass entries rebuild: above this, every open collector is flushed

let status = { running: false }

// Same rule as sourceRecords.replaceRecords: non-object payloads (e.g. XML) are stored as { raw } and not indexed
function isWrappedRaw(doc) {
    return doc.raw !== undefined && (doc.raw === null || typeof doc.raw !== "object")
}

// Origin of a document (its refs' origin); documents without one are grouped under "<connector>:no-origin"
function originOf(doc, connector) {
    const value = doc[collectionSettings(connector).originField]
    return typeof value === "string" && value ? value : connector + ":no-origin"
}

async function getOrigins(connector, origin) {
    const { originField } = collectionSettings(connector)
    return origin ? [origin] : (await collectionModel(connector).distinct(originField)).filter(o => typeof o === "string" && o)
}

// One origin: its refs are removed and written again from its documents
async function rebuildOriginEntries(connector, origin) {
    await ensureOriginIndex(connector)
    await removeOrigin(origin)
    const collector = createCollector(origin, connector)
    let count = 0
    const { originField } = collectionSettings(connector)
    for await (const doc of collectionModel(connector).find({ [originField]: origin }).lean().cursor())
        if (!isWrappedRaw(doc)) {
            await collector.add([doc])
            count++
        }
    await collector.flush()
    logger.info(`Entries rebuilt for ${origin} (${connector}) from ${count} documents`)
    return { origins: 1, documents: count }
}

// The whole collection in one pass (no index needed, even on tens of millions of datapoints): the connector's refs
// are removed first, then every document is indexed under its origin.
async function rebuildConnectorEntries(connector) {
    await removeRefs({ connectors: [connector] })
    const collectors = new Map()
    let count = 0
    for await (const doc of collectionModel(connector).find({}).lean().cursor()) {
        if (isWrappedRaw(doc))
            continue
        const origin = originOf(doc, connector)
        let collector = collectors.get(origin)
        if (!collector) {
            if (collectors.size >= MAX_OPEN_COLLECTORS)
                for (const [o, c] of collectors) {
                    await c.flush()
                    collectors.delete(o)
                }
            collector = createCollector(origin, connector)
            collectors.set(origin, collector)
        }
        await collector.add([doc])
        if (++count % 100000 == 0)
            logger.info(`Entries of ${connector}: ${count} documents read`)
    }
    for (const collector of collectors.values())
        await collector.flush()
    logger.info(`Entries rebuilt for ${connector} from ${count} documents`)
    return { documents: count }
}

async function rebuildEntries({ connector, origin, keepLegacy } = {}) {
    if (!keepLegacy) {
        for (const Model of [Key, Values, Entries]) {
            const { deletedCount } = await Model.collection.deleteMany({ refs: { $exists: false } })
            logger.info(`${Model.modelName}: ${deletedCount} legacy docs without refs deleted`)
        }
        await removeRefs({ withoutConnector: true })
    }
    const result = {}
    for (const c of connector ? [connector] : REBUILT_CONNECTORS) {
        if (!collectionSettings(c).toMongo) {
            result[c] = "skipped: not stored in MongoDB (collections." + c + ".toMongo)"
            continue
        }
        logger.info(`Rebuilding the entries of ${c}${origin ? " for " + origin : ""}`)
        result[c] = origin ? await rebuildOriginEntries(c, origin) : await rebuildConnectorEntries(c)
    }
    return result
}

// Dedicated connection: the service's shared pg client is also used by the connectors, and a transaction on it
// would include (and on rollback, undo) their concurrent writes.
async function mirrorConnector(pg, connector, origin) {
    const { postgres: table, originField } = collectionSettings(connector)
    const failed = []
    await pg.query(`CREATE TABLE IF NOT EXISTS ${table} (id SERIAL PRIMARY KEY, name TEXT, data JSONB, record JSONB)`)

    async function insertBatch(rows, o) {
        if (!rows.length)
            return
        const values = []
        const placeholders = rows.map((row, i) => {
            const { _id, ...data } = row
            values.push(data.name || data.id || o, data, { from: o })
            return `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`
        })
        await pg.query(`INSERT INTO ${table} (name, data, record) VALUES ${placeholders.join(", ")}`, values)
    }

    const origins = await getOrigins(connector, origin)
    await ensureOriginIndex(connector)
    let index = 0
    for (const o of origins) {
        logger.info(`Postgres ${table} ${++index}/${origins.length}: ${o}`)
        // one transaction per origin: readers never see the origin half-empty
        await pg.query("BEGIN")
        try {
            await pg.query(`DELETE FROM ${table} WHERE record->>'from' = $1`, [o])
            let batch = []
            let count = 0
            for await (const doc of collectionModel(connector).find({ [originField]: o }).lean().cursor()) {
                batch.push(doc)
                count++
                if (batch.length >= PG_BATCH_SIZE) {
                    await insertBatch(batch, o)
                    batch = []
                }
            }
            await insertBatch(batch, o)
            await pg.query("COMMIT")
            logger.info(`Postgres ${table} mirrored for ${o}: ${count} rows`)
        }
        catch (error) {
            await pg.query("ROLLBACK")
            failed.push(o)
            logger.error(`Postgres mirror of ${table} failed for ${o}, rolled back`, error)
        }
    }

    let orphansDeleted = 0
    if (!origin) {
        // rows of origins that no longer exist in MongoDB
        const { rowCount } = await pg.query(
            `DELETE FROM ${table} WHERE record->>'from' IS NOT NULL AND NOT (record->>'from' = ANY($1::text[]))`,
            [origins]
        )
        orphansDeleted = rowCount
        logger.info(`Postgres ${table}: ${rowCount} rows of origins no longer in MongoDB deleted`)
    }
    return { table, origins: origins.length, failed, orphansDeleted }
}

async function mirrorPostgres({ connector, origin } = {}) {
    const result = {}
    let pg
    try {
        for (const c of connector ? [connector] : REBUILT_CONNECTORS) {
            const settings = collectionSettings(c)
            if (!settings.toPostgres)
                result[c] = `skipped: collections.${c}.toPostgres is off (or queryOptions.SQLQuery is disabled)`
            else if (!settings.toMongo)
                result[c] = `skipped: the mirror is made from MongoDB, but collections.${c}.toMongo is off`
            else {
                if (!pg) {
                    pg = new Client(config.postgreConfig)
                    await pg.connect()
                }
                result[c] = await mirrorConnector(pg, c, origin)
            }
            if (typeof result[c] === "string")
                logger.warn(`Postgres mirror of ${c}: ${result[c]}`)
        }
        return result
    }
    finally {
        await pg?.end().catch(() => { })
    }
}

function validate({ mode, origin, connector } = {}) {
    if (mode === undefined || mode === null || mode === "")
        return `mode is required (${MODES.join(" | ")})`
    if (!MODES.includes(mode))
        return `Unknown mode "${mode}" (${MODES.join(" | ")})`
    if (connector !== undefined && !REBUILT_CONNECTORS.includes(connector))
        return connector == "minio"
            ? "MinIO is rebuilt by its sync (PUT /api/query), not here"
            : `Unknown connector "${connector}" (${REBUILT_CONNECTORS.join(" | ")})`
    if (origin !== undefined && (typeof origin != "string" || !origin))
        return "origin must be a non-empty string"
    if (origin !== undefined && connector === undefined)
        return "origin requires a connector (api | orion)"
}

// Runs a rebuild holding the shared lock. Throws (with .code) if one is already running or a poll is in progress.
async function runRebuild({ mode, origin, connector, keepLegacy = false } = {}) {
    const invalid = validate({ mode, origin, connector })
    if (invalid)
        throw Object.assign(new Error(invalid), { code: "INVALID" })
    if (locks.rebuilding)
        throw Object.assign(new Error("A rebuild is already running"), { code: "BUSY" })
    if (locks.polling)
        throw Object.assign(new Error("An API poll is running, retry when it has finished"), { code: "BUSY" })

    locks.rebuilding = true
    status = { running: true, mode, connector: connector || null, origin: origin || null, keepLegacy, startedAt: new Date() }
    try {
        const result = {}
        if (mode == "entries" || mode == "all")
            result.entries = await rebuildEntries({ connector, origin, keepLegacy })
        if (mode == "postgres" || mode == "all")
            result.postgres = await mirrorPostgres({ connector, origin })
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
    REBUILT_CONNECTORS,
    rebuildEntries,
    mirrorPostgres,
    validate,
    runRebuild,
    getRebuildStatus: () => status
}
