// Shared "record" ingestion used by apiConnector and by the Orion notification path (service.js):
// records go to the `sources` collection with `source: <origin url>`, keys/values/entries via entriesStore,
// and (if enabled) to the PostgreSQL `sources` table with record.from = <origin url>.

const config = require('../config')
const logger = require('percocologger')
const Source = require("../api/models/Models").Source
const client = require('../inputConnectors/postgresConnector')
const { createCollector, removeOrigin } = require('./entriesStore')

async function waitForPostgreInit() {
    while (!process.postgreInit || process.postgreInit === "busy")
        await new Promise(resolve => setTimeout(resolve, 1000))
}

function pgQuery(query, params) {
    return new Promise((resolve, reject) => client.query(query, params, (err, res) => err ? reject(err) : resolve(res)))
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

function makeItem(item, source) {
    let sourceId = item.id
    delete item.id
    prepareBackupValues(item, "source")
    prepareBackupValues(item, "sourceId")
    return { ...item, source: source, sourceId: sourceId }
}

async function insertToPostgre(data, name, batchValue, url) {
    if (config.queryOptions.SQLQuery && data.length > 0) {
        await waitForPostgreInit()
        const values = []
        for (const [i, item] of data.entries()) {
            const sourceName =
                item.name ||
                item.id ||
                batchValue ||
                url//`${batchUrl}${Date.now()}-${i}`

            values.push(
                sourceName,
                item,
                { from: url }
            )
        }

        const placeholders = data.map((_, i) => {
            const n = i * 3
            return `($${n + 1}, $${n + 2}, $${n + 3})`
        }).join(', ')

        const query = `INSERT INTO sources (name, data, record) VALUES ${placeholders}`
        client.query(query, values, (err, res) => {
            if (err) {
                logger.error(`Error inserting records for ${name} API`, err)
                return
            }
            logger.info(`Inserted ${data.length} records for ${name} API`)
        })
    }
}

// Replaces everything previously ingested from `origin` with `data` (upsert semantics):
// sources (Mongo + Postgres) and keys/values/entries refs of that origin.
// Non-object items (e.g. an XML/text payload) are stored as { raw, name } and not indexed in keys/values/entries.
async function replaceRecords(data, origin, { logName = origin, sqlName } = {}) {
    const items = Array.isArray(data) ? data : [data]
    const records = items.map(item => item !== null && typeof item === "object" ? item : { raw: item, name: sqlName })

    await Source.deleteMany({ source: origin })
    await removeOrigin(origin)
    if (config.queryOptions.SQLQuery) {
        await waitForPostgreInit()
        await pgQuery(`DELETE FROM sources WHERE record->>'from' = $1`, [origin])
    }

    const insertingData = records.map(item => makeItem(item, origin))
    await Source.insertMany(insertingData)

    const collector = createCollector(origin)
    await collector.add(insertingData.filter((_, i) => records[i] === items[i])) // skip wrapped raw payloads
    await collector.flush()

    await insertToPostgre(insertingData, logName, sqlName, origin)
    logger.info(`Replaced records of ${origin}: ${insertingData.length}`)
}

module.exports = {
    waitForPostgreInit,
    pgQuery,
    prepareBackupValues,
    makeItem,
    insertToPostgre,
    replaceRecords
}
