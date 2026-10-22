// Paginated API polls (apiConnector): every record stored by a page is referenced in the status collection (MongoDB)
// and in the status table (PostgreSQL) with pending: true, together with the url of the API and the datePolled of
// the poll. When the last page has been stored the references of that poll (url + datePolled) are deleted.
//
// A poll that stops before its last page (an error, a restart of the service) leaves its references: the next poll
// of that url first deletes those records (rollback) and their references, then starts again. So the records of an
// incomplete poll never count as polled (the incremental polls start from the last complete one) and are not stored
// twice.
//
//   MongoDB    status: { _id: <_id of the record>, url, pending: true, datePolled }
//   PostgreSQL status: (id, record_id = id of the row in the API table, record_table, url, pending, date_polled)

const logger = require('percocologger')
const Status = require('../api/models/Status')
const { pgQuery, waitForPostgreInit } = require('./sourceRecords')
const { collectionSettings, collectionModel } = require('./collections')
const { rebuildEntries } = require('./rebuild')

const PG_TABLE = "status"
let pgTableReady

function ensurePgStatusTable() {
    pgTableReady ||= (async () => {
        await waitForPostgreInit()
        await pgQuery(`CREATE TABLE IF NOT EXISTS ${PG_TABLE} (
            id SERIAL PRIMARY KEY,
            record_id INTEGER NOT NULL,
            record_table TEXT NOT NULL,
            url TEXT NOT NULL,
            pending BOOLEAN NOT NULL DEFAULT true,
            date_polled TIMESTAMPTZ NOT NULL
        )`)
        await pgQuery(`CREATE INDEX IF NOT EXISTS ${PG_TABLE}_url_date_polled_idx ON ${PG_TABLE} (url, date_polled)`)
    })().catch(error => {
        pgTableReady = undefined
        throw error
    })
    return pgTableReady
}

// The records just stored by a page (storeRecords: { mongo: [_id], postgres: [id] }) are pending
async function markPending(url, datePolled, { mongo = [], postgres = [] } = {}) {
    const settings = collectionSettings("api")
    if (settings.toMongo && mongo.length)
        await Status.collection.insertMany(mongo.map(_id => ({ _id, url, pending: true, datePolled })))
    if (settings.toPostgres && postgres.length) {
        await ensurePgStatusTable()
        const values = []
        const placeholders = postgres.map((id, i) => {
            values.push(id, settings.postgres, url, datePolled)
            return `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, true, $${i * 4 + 4})`
        })
        await pgQuery(`INSERT INTO ${PG_TABLE} (record_id, record_table, url, pending, date_polled) VALUES ${placeholders.join(", ")}`, values)
    }
}

// The last page of the poll has been stored: its references are deleted
async function completePoll(url, datePolled) {
    const settings = collectionSettings("api")
    if (settings.toMongo)
        await Status.collection.deleteMany({ url, datePolled })
    if (settings.toPostgres) {
        await ensurePgStatusTable()
        await pgQuery(`DELETE FROM ${PG_TABLE} WHERE url = $1 AND date_polled = $2`, [url, datePolled])
    }
}

// Before a poll: the records left pending by an incomplete poll of `url` are deleted, with their references.
// The keys/values/entries of the url are rebuilt from the records that remain (the deleted ones had added theirs).
// Returns how many records were deleted.
async function rollbackPending(url, { logName = url } = {}) {
    const settings = collectionSettings("api")
    const deleted = { mongo: 0, postgres: 0 }
    if (settings.toMongo) {
        const ids = (await Status.collection.find({ url, pending: true }).toArray()).map(doc => doc._id)
        if (ids.length)
            deleted.mongo = (await collectionModel("api").collection.deleteMany({ _id: { $in: ids } })).deletedCount
        await Status.collection.deleteMany({ url })
    }
    if (settings.toPostgres) {
        await ensurePgStatusTable()
        const { rowCount } = await pgQuery(
            `DELETE FROM ${settings.postgres} WHERE id IN (SELECT record_id FROM ${PG_TABLE} WHERE url = $1 AND pending AND record_table = $2)`,
            [url, settings.postgres])
        deleted.postgres = rowCount
        await pgQuery(`DELETE FROM ${PG_TABLE} WHERE url = $1`, [url])
    }
    if (deleted.mongo || deleted.postgres) {
        logger.warn(`${logName}: the previous poll did not finish, its records were deleted (MongoDB ${deleted.mongo}, PostgreSQL ${deleted.postgres})`)
        if (deleted.mongo)
            await rebuildEntries({ connector: "api", origin: url, keepLegacy: true })
    }
    return deleted
}

module.exports = { markPending, completePoll, rollbackPending, ensurePgStatusTable, PG_TABLE }
