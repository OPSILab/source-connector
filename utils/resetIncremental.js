// Starts the incremental APIs again from scratch (e.g. to get rid of duplicated records): for each incremental API of
// apiConnectorConfig.apiUrls (or the ones named), its records are deleted - MongoDB collection, PostgreSQL table,
// keys / values / entries refs, the pending poll references (utils/pollStatus.js) - and the API is polled again at
// once, as the first time: from startDateValue, in maxWindow windows, every page (inputConnectors/apiConnector.js).
// Used by POST /api/incremental/reset and by scripts/resetIncrementalApis.js.
//
// The shared lock (utils/jobLocks.js): refused while a poll or a rebuild runs; while deleting, the scheduled polls
// are skipped (locks.rebuilding), then the poll of these APIs holds locks.polling as any poll does.
// APIs with `batch` are not incremental for apiConnector (their requests ignore incrementalParams): left out.
const config = require("../config")
const logger = require("percocologger")
const locks = require("./jobLocks")
const pollStatus = require("./pollStatus")
const { clearOrigin, pgQuery, waitForPostgreInit, ensurePgTable } = require("./sourceRecords")
const { collectionSettings, collectionModel } = require("./collections")

let status = { running: false }

// The incremental APIs to reset: all of them, or those named (an unknown or not incremental name: an error)
function selectApis(names) {
    const apis = (config.apiConnectorConfig?.apiUrls || []).filter(api => api?.incremental && !api.batch && api.url)
    if (!names?.length)
        return apis
    const unknown = names.filter(name => !apis.some(api => api.name == name))
    if (unknown.length)
        throw Object.assign(new Error(`Not incremental APIs of apiConnectorConfig.apiUrls: ${unknown.join(", ")} (incremental: ${apis.map(api => api.name).join(", ") || "none"})`), { code: "INVALID" })
    return apis.filter(api => names.includes(api.name))
}

// Records of an API in MongoDB and PostgreSQL
async function countRecords(url) {
    const settings = collectionSettings("api")
    const counts = {}
    if (settings.toMongo)
        counts.mongo = await collectionModel("api").collection.countDocuments({ [settings.originField]: url })
    if (settings.toPostgres) {
        await waitForPostgreInit()
        await ensurePgTable(settings.postgres)
        counts.postgres = Number((await pgQuery(`SELECT count(*) AS n FROM ${settings.postgres} WHERE record->>'from' = $1`, [url])).rows[0].n)
    }
    return counts
}

// names: API names (none: every incremental API). pollAPI: apiConnector's (it polls the APIs it is given).
// dryRun: only the counts, nothing deleted nor polled.
// Answer { apis: [{ name, url, before: { mongo, postgres }, after }] }
async function resetIncremental({ names, pollAPI, dryRun = false } = {}) {
    const apis = selectApis(names)
    if (dryRun) {
        const result = []
        for (const api of apis)
            result.push({ name: api.name, url: api.url, records: await countRecords(api.url) })
        return { dryRun: true, apis: result }
    }
    if (typeof pollAPI != "function")
        throw Object.assign(new Error("apiConnector is not running (sourceConnectors.apiConnector)"), { code: "INVALID" })
    if (locks.rebuilding)
        throw Object.assign(new Error("A rebuild is running"), { code: "BUSY" })
    if (locks.polling)
        throw Object.assign(new Error("An API poll is running, retry when it has finished"), { code: "BUSY" })

    status = { running: true, phase: "deleting", apis: apis.map(api => api.name), startedAt: new Date() }
    const result = []
    locks.rebuilding = true // no scheduled poll while the records are deleted
    try {
        for (const api of apis) {
            const before = await countRecords(api.url)
            await pollStatus.rollbackPending(api.url, { logName: api.name })
            await clearOrigin(api.url, "api")
            logger.info(`${api.name}: records deleted (MongoDB ${before.mongo ?? "-"}, PostgreSQL ${before.postgres ?? "-"}), polled again from the start`)
            result.push({ name: api.name, url: api.url, before })
        }
    }
    catch (error) {
        status = { ...status, running: false, finishedAt: new Date(), error: error.message || String(error) }
        throw error
    }
    finally {
        locks.rebuilding = false
    }
    status = { ...status, phase: "polling" }
    try {
        // no await between the lock released above and pollAPI taking locks.polling: no scheduled poll in between
        await pollAPI(apis)
        for (const entry of result)
            entry.after = await countRecords(entry.url)
        status = { ...status, running: false, phase: "done", finishedAt: new Date(), result }
        logger.info("Incremental APIs reset", result)
        return { apis: result }
    }
    catch (error) {
        status = { ...status, running: false, finishedAt: new Date(), error: error.message || String(error) }
        throw error
    }
}

module.exports = { resetIncremental, selectApis, getResetStatus: () => status }
