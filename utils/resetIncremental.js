// Starts the incremental APIs again from scratch (e.g. to get rid of duplicated records): for each incremental API of
// apiConnectorConfig.apiUrls (or the ones named), its records are deleted - MongoDB collection, PostgreSQL table,
// keys / values / entries refs, the pending poll references (utils/pollStatus.js), the state of its polls (end of the
// last window stored, windows answered 404: utils/incrementalState.js) - and the API is polled again at
// once, as the first time: from startDateValue, in maxWindow windows, every page (inputConnectors/apiConnector.js).
// Used by POST /api/incremental/reset and by scripts/resetIncrementalApis.js.
//
// The shared lock (utils/jobLocks.js): refused while a poll or a rebuild runs; while deleting, the scheduled polls
// are skipped (locks.rebuilding), then the poll of these APIs holds locks.polling as any poll does.
// Every reset (not the dry runs) is logged in the collection "incrementalresetlog" (api/models/IncrementalResetLog.js):
// who, when, which APIs, the counts before / after, the outcome. listResets() reads it (GET /incremental/reset/log).
// A batch API (url with {batch}) stores its records under the url of each element: all of them are deleted (the
// origins matching the url with any value in place of {batch}), and every element starts again from startDateValue.
const config = require("../config")
const logger = require("percocologger")
const locks = require("./jobLocks")
const pollStatus = require("./pollStatus")
const Status = require("../api/models/Status")
const incrementalState = require("./incrementalState")
const { clearOrigin, pgQuery, waitForPostgreInit, ensurePgTable } = require("./sourceRecords")
const { collectionSettings, collectionModel } = require("./collections")
const IncrementalResetLog = require("../api/models/IncrementalResetLog")
const mongoose = require("mongoose")

let status = { running: false }

// The incremental APIs to reset: all of them, or those named (an unknown or not incremental name: an error)
function selectApis(names) {
    const apis = (config.apiConnectorConfig?.apiUrls || []).filter(api => api?.incremental && api.url)
    if (!names?.length)
        return apis
    const unknown = names.filter(name => !apis.some(api => api.name == name))
    if (unknown.length)
        throw Object.assign(new Error(`Not incremental APIs of apiConnectorConfig.apiUrls: ${unknown.join(", ")} (incremental: ${apis.map(api => api.name).join(", ") || "none"})`), { code: "INVALID" })
    return apis.filter(api => names.includes(api.name))
}

const escapeRegex = text => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

// A batch API: the regular expression of the urls of its elements (api.url with anything in place of {batch})
function batchPattern(api) {
    return api.batch ? "^" + escapeRegex(api.url).replace(escapeRegex("{batch}"), ".+") + "$" : undefined
}

// The origins of the records of an API: its url; for a batch API the url of every element found in MongoDB,
// PostgreSQL or among the pending polls
async function originsOf(api) {
    const pattern = batchPattern(api)
    if (!pattern)
        return [api.url]
    const settings = collectionSettings("api")
    const origins = new Set()
    const add = values => values.forEach(value => value && origins.add(value))
    if (settings.toMongo) {
        add(await collectionModel("api").collection.distinct(settings.originField, { [settings.originField]: { $regex: pattern } }))
        add(await Status.collection.distinct("url", { url: { $regex: pattern } }))
    }
    add(await incrementalState.urlsMatching(pattern)) // in MongoDB in any case
    if (settings.toPostgres) {
        await waitForPostgreInit()
        await ensurePgTable(settings.postgres)
        add((await pgQuery(`SELECT DISTINCT record->>'from' AS url FROM ${settings.postgres} WHERE record->>'from' ~ $1`, [pattern])).rows.map(row => row.url))
        await pollStatus.ensurePgStatusTable()
        add((await pgQuery(`SELECT DISTINCT url FROM ${pollStatus.PG_TABLE} WHERE url ~ $1`, [pattern])).rows.map(row => row.url))
    }
    return [...origins].sort()
}

// Records of an API in MongoDB and PostgreSQL (a batch API: of all its elements)
async function countRecords(api) {
    const settings = collectionSettings("api")
    const pattern = batchPattern(api)
    const counts = {}
    if (settings.toMongo)
        counts.mongo = await collectionModel("api").collection.countDocuments({ [settings.originField]: pattern ? { $regex: pattern } : api.url })
    if (settings.toPostgres) {
        await waitForPostgreInit()
        await ensurePgTable(settings.postgres)
        counts.postgres = Number((await pgQuery(`SELECT count(*) AS n FROM ${settings.postgres} WHERE record->>'from' ${pattern ? "~" : "="} $1`, [pattern || api.url])).rows[0].n)
    }
    return counts
}

// The log of a reset: never makes the reset fail (a failed write is only logged)
async function writeLog(action) {
    try {
        return await action()
    }
    catch (error) {
        logger.error("incrementalresetlog not written:", error?.message || error)
    }
}

// names: API names (none: every incremental API). pollAPI: apiConnector's (it polls the APIs it is given).
// dryRun: only the counts, nothing deleted nor polled. by: who asked (logged).
// Answer { apis: [{ name, url, before: { mongo, postgres }, after }] }
async function resetIncremental({ names, pollAPI, dryRun = false, by } = {}) {
    const apis = selectApis(names)
    if (dryRun) {
        const result = []
        for (const api of apis)
            result.push({ name: api.name, url: api.url, records: await countRecords(api) })
        return { dryRun: true, apis: result }
    }
    if (typeof pollAPI != "function")
        throw Object.assign(new Error("apiConnector is not running (sourceConnectors.apiConnector)"), { code: "INVALID" })
    if (locks.rebuilding)
        throw Object.assign(new Error("A rebuild is running"), { code: "BUSY" })
    if (locks.polling)
        throw Object.assign(new Error("An API poll is running, retry when it has finished"), { code: "BUSY" })

    status = { running: true, phase: "deleting", apis: apis.map(api => api.name), startedAt: new Date() }
    const logId = new mongoose.Types.ObjectId()
    const logged = await writeLog(() => IncrementalResetLog.collection.insertOne({
        _id: logId, startedAt: status.startedAt, by: by || "unknown", apis: status.apis, outcome: "running"
    }).then(() => true))
    const finish = fields => writeLog(() => logged && IncrementalResetLog.collection.updateOne({ _id: logId }, { $set: { finishedAt: new Date(), ...fields } }))
    const result = []
    locks.rebuilding = true // no scheduled poll while the records are deleted
    try {
        for (const api of apis) {
            const before = await countRecords(api)
            const origins = await originsOf(api)
            for (const origin of origins) {
                await pollStatus.rollbackPending(origin, { logName: api.name })
                await clearOrigin(origin, "api")
            }
            await incrementalState.clear(origins)
            logger.info(`${api.name}: records deleted (MongoDB ${before.mongo ?? "-"}, PostgreSQL ${before.postgres ?? "-"}${api.batch ? `, ${origins.length} elements` : ""}), polled again from the start`)
            result.push({ name: api.name, url: api.url, before, ...(api.batch ? { elements: origins.length } : {}) })
        }
    }
    catch (error) {
        status = { ...status, running: false, finishedAt: new Date(), error: error.message || String(error) }
        await finish({ outcome: "error", error: status.error, result })
        throw error
    }
    finally {
        locks.rebuilding = false
    }
    status = { ...status, phase: "polling" }
    try {
        // no await between the lock released above and pollAPI taking locks.polling: no scheduled poll in between
        await pollAPI(apis)
        for (const [i, entry] of result.entries())
            entry.after = await countRecords(apis[i])
        status = { ...status, running: false, phase: "done", finishedAt: new Date(), result }
        await finish({ outcome: "done", result })
        logger.info("Incremental APIs reset", result)
        return { apis: result }
    }
    catch (error) {
        status = { ...status, running: false, finishedAt: new Date(), error: error.message || String(error) }
        await finish({ outcome: "error", error: status.error, result })
        throw error
    }
}

// The logged resets, newest first (limit: the latest ones). A reset still "running" while none is running here (the
// Source-Connector stopped during it): "interrupted".
async function listResets({ limit = 100 } = {}) {
    const docs = await IncrementalResetLog.collection.find({}).sort({ startedAt: -1, _id: -1 }).limit(limit).toArray()
    return docs.map(({ _id, ...doc }) => ({ id: String(_id), ...doc, ...(doc.outcome == "running" && !status.running ? { outcome: "interrupted" } : {}) }))
}

module.exports = { resetIncremental, selectApis, listResets, getResetStatus: () => status }
