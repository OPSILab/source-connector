// Record ingestion shared by apiConnector and by the Orion notification path (service.js): records go to the
// connector's MongoDB collection with their origin url (api: `source`, orion: `fromUrl`), keys/values/entries via
// entriesStore, and (if enabled) to the connector's PostgreSQL table with record.from = <origin url>.
// Where each connector writes: config.collections (utils/collections.js).

const logger = require('percocologger')
const client = require('../inputConnectors/postgresConnector')
const { createCollector, removeOrigin } = require('./entriesStore')
const { collectionSettings, collectionModel } = require('./collections')

async function waitForPostgreInit() {
    while (!process.postgreInit || process.postgreInit === "busy")
        await new Promise(resolve => setTimeout(resolve, 1000))
}

function pgQuery(query, params) {
    return new Promise((resolve, reject) => client.query(query, params, (err, res) => err ? reject(err) : resolve(res)))
}

// The table of a connector, created the first time it is used (postgresConnector creates only `sources`)
const tablesReady = new Map()
function ensurePgTable(table) {
    if (!tablesReady.has(table))
        tablesReady.set(table, pgQuery(`CREATE TABLE IF NOT EXISTS ${table} (id SERIAL PRIMARY KEY, name TEXT, data JSONB, record JSONB)`)
            .catch(error => {
                tablesReady.delete(table)
                throw error
            }))
    return tablesReady.get(table)
}

function prepareBackupValues(item, value) {
    if (item[value] === undefined)
        return

    const original = value + "_original"

    if (item[original] !== undefined) {
        if (typeof item[original] === "string" && typeof item[value] === "string")
            item[original] += " | " + item[value]
        else {
            let originalValue
            if (item[original] !== null && typeof item[original] === "object")
                originalValue = JSON.parse(JSON.stringify(item[original]))
            else
                originalValue = item[original]
            item[original] = {
                original: originalValue,
                [value]: item[value]
            }
        }
    } else {
        item[original] = item[value]
    }
}

// The document a record becomes, with its origin:
//  - api: `source` = origin, id -> sourceId; without an id no sourceId field at all (a `sourceId` of the record
//    is kept in sourceId_original, like a `source` in source_original);
//  - orion: `fromUrl` = origin (a `fromUrl` of the record is kept in fromUrl_original).
function makeItem(item, origin, connector = "api") {
    if (connector == "orion") {
        prepareBackupValues(item, "fromUrl")
        return { ...item, fromUrl: origin }
    }
    let sourceId = item.id
    delete item.id
    prepareBackupValues(item, "source")
    prepareBackupValues(item, "sourceId")
    if (sourceId === undefined) {
        delete item.sourceId
        return { ...item, source: origin }
    }
    return { ...item, source: origin, sourceId: sourceId }
}

async function insertToPostgre(data, name, batchValue, url, connector = "api") {
    const { toPostgres, postgres: table } = collectionSettings(connector)
    if (!toPostgres || !data.length)
        return
    await waitForPostgreInit()
    await ensurePgTable(table)
    const values = []
    for (const item of data) {
        const sourceName =
            item.name ||
            item.id ||
            batchValue ||
            url
        values.push(sourceName, item, { from: url })
    }
    const placeholders = data.map((_, i) => {
        const n = i * 3
        return `($${n + 1}, $${n + 2}, $${n + 3})`
    }).join(', ')
    try {
        await pgQuery(`INSERT INTO ${table} (name, data, record) VALUES ${placeholders}`, values)
        logger.info(`Inserted ${data.length} records for ${name} in PostgreSQL ${table}`)
    }
    catch (error) {
        logger.error(`Error inserting records for ${name} in PostgreSQL ${table}`, error)
    }
}

// Removes the PostgreSQL rows of an origin (no-op when the connector does not write to PostgreSQL)
async function deletePostgresOrigin(origin, connector = "api") {
    const { toPostgres, postgres: table } = collectionSettings(connector)
    if (!toPostgres)
        return
    await waitForPostgreInit()
    await ensurePgTable(table)
    await pgQuery(`DELETE FROM ${table} WHERE record->>'from' = $1`, [origin])
}

// The lookups by origin (replace, rebuild) need this index: without it every replace scans the collection.
// On a big existing collection (datapoints) the first call builds it: scripts/migrateCollections.js does it upfront.
const originIndexes = new Map()
function ensureOriginIndex(connector = "api") {
    const { originField, mongo } = collectionSettings(connector)
    const key = connector + ":" + mongo
    if (originField && !originIndexes.has(key))
        originIndexes.set(key, collectionModel(connector).collection.createIndex({ [originField]: 1 }).catch(error => {
            originIndexes.delete(key)
            logger.error(`Error creating the ${mongo}.${originField} index`, error)
        }))
    return originIndexes.get(key)
}

// Everything of `origin`: MongoDB documents and their keys/values/entries refs, PostgreSQL rows
async function clearOrigin(origin, connector = "api") {
    const settings = collectionSettings(connector)
    if (settings.toMongo) {
        await ensureOriginIndex(connector)
        await collectionModel(connector).deleteMany({ [settings.originField]: origin })
        await removeOrigin(origin)
    }
    await deletePostgresOrigin(origin, connector)
}

// Stores records already made with makeItem: MongoDB + keys/values/entries (collector) and PostgreSQL, as configured
async function storeRecords(records, origin, { connector = "api", collector, logName = origin, sqlName } = {}) {
    if (!records.length)
        return
    const settings = collectionSettings(connector)
    if (settings.toMongo) {
        if (connector == "orion")
            // the Orion collection has the Datapoint schema: unique dupl_hash (with upsertRecords), so every document
            // needs one, the same way the datapoints get it
            await collectionModel(connector).upsertMany(records)
        else
            await collectionModel(connector).insertMany(records)
        await collector?.add(records)
    }
    await insertToPostgre(records, logName, sqlName, origin, connector)
}

// Like replaceRecords, for data that arrives in blocks: removes everything of `origin`, then add(items) stores each
// block; close() writes the last keys/values/entries.
async function openReplace(origin, { logName = origin, sqlName, connector = "api" } = {}) {
    await clearOrigin(origin, connector)
    const collector = createCollector(origin, connector)
    let count = 0
    return {
        async add(items) {
            const records = items.map(item => makeItem(item, origin, connector))
            await storeRecords(records, origin, { connector, collector, logName, sqlName })
            count += records.length
        },
        async close() {
            await collector.flush()
            logger.info(`Replaced records of ${origin} (${connector}): ${count}`)
            return count
        }
    }
}

// Replaces everything previously ingested from `origin` with `data` (upsert semantics).
// Non-object items (e.g. an XML/text payload) are stored as { raw, name } and not indexed in keys/values/entries.
async function replaceRecords(data, origin, { logName = origin, sqlName, connector = "api" } = {}) {
    const items = Array.isArray(data) ? data : [data]
    const records = items.map(item => item !== null && typeof item === "object" ? item : { raw: item, name: sqlName })
    await clearOrigin(origin, connector)
    const insertingData = records.map(item => makeItem(item, origin, connector))
    await storeRecords(insertingData, origin, { connector, logName, sqlName })
    if (collectionSettings(connector).toMongo) {
        const collector = createCollector(origin, connector)
        await collector.add(insertingData.filter((_, i) => records[i] === items[i])) // skip wrapped raw payloads
        await collector.flush()
    }
    logger.info(`Replaced records of ${origin} (${connector}): ${insertingData.length}`)
}

module.exports = {
    waitForPostgreInit,
    pgQuery,
    ensurePgTable,
    prepareBackupValues,
    makeItem,
    insertToPostgre,
    deletePostgresOrigin,
    ensureOriginIndex,
    clearOrigin,
    storeRecords,
    openReplace,
    replaceRecords
}
