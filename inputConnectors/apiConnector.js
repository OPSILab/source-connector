const config = require('../config')
const logger = require('percocologger')
const axios = require('axios')
const { createCollector } = require('../utils/entriesStore')
const { makeItem, clearOrigin, storeRecords, pgQuery, waitForPostgreInit, ensurePgTable } = require('../utils/sourceRecords')
const { collectionSettings, collectionModel } = require('../utils/collections')
const locks = require('../utils/jobLocks')
const pollStatus = require('../utils/pollStatus')
const incrementalState = require('../utils/incrementalState')
const snapshot = require('../utils/snapshotRecords')
let tokens = {}

const DAY = 1000 * 60 * 60 * 24
const YMD = "YYYY-MM-DD"
const utcMidnight = date => new Date(date.toISOString().split("T")[0])

// incrementalParams.maxWindow (ms): the longest time span of one request; undefined when not set or not usable (warned).
// With YYYY-MM-DD dates or alignToDays it is a whole number of days, at least one.
function maxWindowOf(api) {
    const params = api.incrementalParams
    const maxWindow = params.maxWindow
    if (maxWindow === undefined || maxWindow === null)
        return undefined
    if (!Number.isFinite(maxWindow) || maxWindow <= 0) {
        logger.warn(`${api.name}: maxWindow must be a positive number of milliseconds, ignored (${maxWindow})`)
        return undefined
    }
    if (!params.endDateParam) {
        logger.warn(`${api.name}: maxWindow needs an endDateParam to split the requests, ignored`)
        return undefined
    }
    if (params.startDateFormat == YMD || params.endDateFormat == YMD || params.alignToDays === true) {
        const days = Math.max(1, Math.floor(maxWindow / DAY))
        if (days * DAY != maxWindow)
            logger.warn(`${api.name}: maxWindow ${maxWindow} ms with ${params.alignToDays === true ? "alignToDays" : "YYYY-MM-DD dates"}: ${days} day(s)`)
        return days * DAY
    }
    return maxWindow
}

// [from, b1, b2, ..., to]: the bounds of the windows from `from` to `to`, each at most maxWindow long. With YYYY-MM-DD
// dates the inner bounds are midnights (UTC) and the last window lasts at least a day, so that the dates of two
// windows follow one another as those of two daily polls do (setDateForIncremental).
function windowBounds(from, to, maxWindow, days) {
    const bounds = [from]
    const limit = days ? utcMidnight(to).getTime() : to.getTime()
    for (let bound = (days ? utcMidnight(from) : from).getTime() + maxWindow; bound < limit; bound += maxWindow)
        bounds.push(new Date(bound))
    bounds.push(to)
    return bounds
}

// The date params of an incremental poll, one object per request: from the end of the last window stored (the state
// of the url, utils/incrementalState.js), else from the last poll (datePolled of the last record), else from
// startDateValue, to now, in windows of at most incrementalParams.maxWindow if set, contiguous as two consecutive
// polls are (start / end logic, pollDelay). ends: the end of each window (stored in the state once it is done).
// mustPull false (no requests): already polled today. lastRecordDate: where the poll starts, for the logs.
// incrementalParams.alignToDays: the windows are whole UTC days, from midnight to the next midnight, and only the days
// already over (at now - pollDelay) are asked: the start is moved back to its midnight (startDateValue too), the end
// is the last midnight; the bounds are not moved by pollDelay again (start / end logic still applied).
function prepareIncremental(lastRecord, api, currentDate, state) {
    const params = api.incrementalParams
    const pollDelay = Number.isFinite(params.pollDelay) ? params.pollDelay : 0
    const aligned = params.alignToDays === true
    const dateOf = (from, to, verse) => setDateForIncremental(from, to, verse == "start" ? params.startDateFormat : params.endDateFormat,
        aligned ? 0 : pollDelay, params.startDateLogic, params.endDateLogic, verse)
    // the end of the poll: now, or with alignToDays the last midnight (UTC) of now - pollDelay
    const until = aligned ? utcMidnight(new Date(currentDate.getTime() - pollDelay)) : currentDate
    const yesterday = new Date(until.getTime() - DAY)
    let from, firstStart
    const sameDay = date => !aligned && date.toISOString().split("T")[0] === currentDate.toISOString().split("T")[0]
    if (state?.lastEnd) {
        from = new Date(state.lastEnd)
        if (state.complete && sameDay(from)) {
            logger.info(`No new records to poll for ${api.name} API (already polled today, until ${from.toISOString()})`)
            return { lastRecordDate: from, mustPull: false, windows: [], ends: [] }
        }
        if (!state.complete)
            logger.info(`${api.name}: the previous poll stopped after the window ending ${from.toISOString()}, resuming from there`)
    }
    else if (lastRecord) {
        from = new Date(lastRecord["datePolled"])
        if (sameDay(from)) {
            logger.info(`No new records to poll for ${api.name} API (last record date: ${from.toISOString()})`)
            return { lastRecordDate: from, mustPull: false, windows: [], ends: [] }
        }
    }
    else if (params.startDateValue) {
        logger.info(`No records found for ${api.name} API, polling from configured start date...`)
        from = new Date(params.startDateValue)
        if (!aligned)
            firstStart = params.startDateValue // sent as configured
    }
    else {
        logger.info(`No records found for ${api.name} API, polling all records...`)
        return { mustPull: true, windows: [params.endDateParam ? { [params.endDateParam]: dateOf(yesterday, until, "end") } : {}], ends: [until] }
    }
    if (aligned && !isNaN(from)) {
        from = utcMidnight(from)
        if (from.getTime() >= until.getTime()) {
            logger.info(`No new records to poll for ${api.name} API (alignToDays: polled until ${from.toISOString()}, the next day is not over yet)`)
            return { lastRecordDate: from, mustPull: false, windows: [], ends: [] }
        }
    }
    const maxWindow = maxWindowOf(api)
    const bounds = maxWindow && !isNaN(from) ?
        windowBounds(from, until, maxWindow, params.startDateFormat == YMD || params.endDateFormat == YMD || aligned) :
        [isNaN(from) ? yesterday : from, until]
    const windows = []
    for (let k = 0; k + 1 < bounds.length; k++) {
        const window = { [params.startDateParam]: k == 0 && firstStart !== undefined ? firstStart : dateOf(bounds[k], bounds[k + 1], "start") }
        if (params.endDateParam)
            window[params.endDateParam] = dateOf(bounds[k], bounds[k + 1], "end")
        windows.push(window)
    }
    if (windows.length > 1)
        logger.info(`${api.name}: ${windows.length} requests of at most ${maxWindow} ms (maxWindow)`)
    return { lastRecordDate: state?.lastEnd || lastRecord ? from : undefined, mustPull: true, windows, ends: bounds.slice(1) }
}

function setStartOffset(type) {
    if (type == "RFC 3339")
        return "1970-01-01T00:00:00Z"
    return 0
}

function setDateForIncremental(lastPollDateIn, currentDate, format, pollDelay, startLogic, endLogic, verse) {
    startLogic = startLogic || "inclusive"
    let lastPollDate = new Date(lastPollDateIn)
    const DAY = 1000 * 60 * 60 * 24
    if ((currentDate.getTime() < lastPollDate.getTime()) || (currentDate.getTime() - lastPollDate.getTime() < DAY) && format == "YYYY-MM-DD")
        lastPollDate.setTime(currentDate.getTime() - DAY)
    let delay = 0
    if (startLogic === "inclusive" && endLogic === "inclusive") {
        if (verse === "start" && format == "YYYY-MM-DD")
            delay = 0//lastPollDate|currentDate - 1000 * 60 * 60 * 24
        else if (verse === "start")
            delay = 0//lastPollDate|currentDate-1
        else if (verse === "end" && format == "YYYY-MM-DD")
            delay = - DAY
        else if (verse === "end")
            delay = - 1
    }
    else if (startLogic === "inclusive" && (!endLogic || endLogic === "exclusive")) {
        if (verse === "start" && format == "YYYY-MM-DD")
            delay = 0//lastPollDate|currentDate - 1000 * 60 * 60 * 24
        else if (verse === "start")
            delay = 0//lastPollDate|currentDate-1
        else if (verse === "end" && format == "YYYY-MM-DD")
            delay = 0
        else if (verse === "end")
            delay = 0
    }
    else if (startLogic === "exclusive" && endLogic === "inclusive") {
        if (verse === "start" && format == "YYYY-MM-DD")
            delay = - DAY//lastPollDate|currentDate - 1000 * 60 * 60 * 24
        else if (verse === "start")
            delay = - 1//lastPollDate|currentDate-1
        else if (verse === "end" && format == "YYYY-MM-DD")
            delay = - DAY
        else if (verse === "end")
            delay = - 1
    }
    else if (startLogic === "exclusive" && (!endLogic || endLogic === "exclusive")) {
        if (verse === "start" && format == "YYYY-MM-DD")
            delay = - DAY//lastPollDate|currentDate - 1000 * 60 * 60 * 24
        else if (verse === "start")
            delay = -1//lastPollDate|currentDate-1
        else if (verse === "end" && format == "YYYY-MM-DD")
            delay = 0
        else if (verse === "end")
            delay = 0
    }
    else
        logger.warn("Unknown start/end logic; using startLogic inclusive && endLogic exclusive")
    if (format === "YYYY-MM-DD")
        return new Date((verse == "start" ? lastPollDate : currentDate).getTime() - pollDelay + delay).toISOString().split("T")[0]
    if (format === "millis")
        return new Date((verse == "start" ? lastPollDate : currentDate).getTime() - pollDelay + delay).getTime()
    if (format == "RFC 3339")
        return new Date((verse == "start" ? lastPollDate : currentDate).getTime() - pollDelay + delay).toISOString()
    logger.warn(`Unknown date format "${format}", using RFC 3339`)
    return new Date((verse == "start" ? lastPollDate : currentDate).getTime() - pollDelay + delay).toISOString()
}

// The API collection (collections.api.mongo, "sources"); its records and PostgreSQL rows as configured
const Source = () => collectionModel("api")

// Records already stored for an origin, in the order they were stored: from MongoDB, or from PostgreSQL when the API
// records are not stored in MongoDB; none when they are stored in neither
async function storedRecords(origin) {
    const settings = collectionSettings("api")
    if (settings.toMongo)
        return await Source().find({ source: origin }).sort({ _id: 1 }).lean()
    if (settings.toPostgres) {
        await waitForPostgreInit()
        await ensurePgTable(settings.postgres)
        return (await pgQuery(`SELECT data FROM ${settings.postgres} WHERE record->>'from' = $1 ORDER BY id`, [origin])).rows.map(row => row.data)
    }
    return []
}

// A non incremental poll of `url` (an API, an element of a batch API, every page of a paginated API). Every record
// stored gets datePolled. upsertRecords (of the API, else of apiConnectorConfig): the records of the url replaced by
// the answer, as before. Otherwise what the answer adds to the records already stored: by order, else as configured
// in unalignedAnswer ("append" / "upsert", of the API, else of apiConnectorConfig), else by count
// (utils/snapshotRecords.js). dropId: the _id of the API records is dropped (they are not stored with it).
async function storeSnapshot(api, url, items, { collector, logName = `${api.name} API`, sqlName, currentDate = new Date(), dropId = false } = {}) {
    const records = items.map(item => item !== null && typeof item === "object" && !Array.isArray(item) ? item : { raw: item })
    if (dropId)
        records.forEach(item => delete item._id)
    const incoming = records.map(item => makeItem({ ...item }, url))
    const incomingKeys = incoming.map(snapshot.recordKey)
    if (api.upsertRecords ?? config.apiConnectorConfig.upsertRecords) {
        await clearOrigin(url, "api")
        await storeRecords(incoming.map(record => ({ ...record, datePolled: currentDate })), url, { collector, logName, sqlName })
        return logger.info(`Replaced the records of ${logName}: ${incoming.length} (upsertRecords)`)
    }
    const stored = await storedRecords(url)
    const storedKeys = stored.map(snapshot.recordKey)
    const unaligned = api.unalignedAnswer ?? config.apiConnectorConfig.unalignedAnswer
    if (unaligned !== undefined && !["append", "upsert"].includes(unaligned))
        logger.warn(`${logName}: unalignedAnswer "${unaligned}" unknown ("append" or "upsert"), by count`)
    let insert, flag
    const overlap = snapshot.alignByOrder(storedKeys, incomingKeys)
    if (!stored.length)
        insert = incoming
    else if (overlap !== null) {
        insert = incoming.slice(overlap)
        flag = { [snapshot.FLAGS.likelyIncremental]: true }
        logger.info(`${logName}: the answer continues the stored records (${overlap} in common), ${insert.length} new`)
    }
    else if (unaligned == "upsert") {
        logger.info(`${logName}: the answer does not continue the stored records, replaced (unalignedAnswer: "upsert")`)
        await clearOrigin(url, "api")
        insert = incoming
    }
    else if (unaligned == "append") {
        logger.info(`${logName}: the answer does not continue the stored records, appended whole (unalignedAnswer: "append")`)
        insert = incoming
        flag = { [snapshot.FLAGS.unverifiedAppend]: true }
    }
    else {
        insert = snapshot.countDelta(storedKeys, incomingKeys).map(i => incoming[i])
        logger.info(`${logName}: the answer does not continue the stored records, by count: ${insert.length} new`)
    }
    if (!insert.length)
        return logger.info(`No new records to insert for ${logName}`)
    await storeRecords(insert.map(record => ({ ...record, datePolled: currentDate, ...flag })), url, { collector, logName, sqlName })
    logger.info(`Inserted ${insert.length} new records for ${logName}`)
}

// The last poll of an incremental API: its most recent record (only datePolled is read). From MongoDB, or from
// PostgreSQL when the API records are not stored in MongoDB (collections.api.toMongo false); null when there is none,
// or when the records are stored in neither (every poll then starts from startDateValue, or from the beginning).
let apiFromIndex // index on the url of the PostgreSQL rows, created once
async function lastPolledRecord(url) {
    const settings = collectionSettings("api")
    if (settings.toMongo)
        // only the records with a date: those stored while the API was not incremental may have none
        return await Source().findOne({ source: url, datePolled: { $exists: true, $ne: null } }).sort({ datePolled: -1 }).lean()
    if (settings.toPostgres) {
        await waitForPostgreInit()
        await ensurePgTable(settings.postgres)
        apiFromIndex ||= pgQuery(`CREATE INDEX IF NOT EXISTS ${settings.postgres}_from_idx ON ${settings.postgres} ((record->>'from'))`)
            .catch(error => {
                apiFromIndex = undefined
                throw error
            })
        await apiFromIndex
        const { rows } = await pgQuery(`SELECT max((data->>'datePolled')::timestamptz) AS "datePolled" FROM ${settings.postgres} WHERE record->>'from' = $1`, [url])
        return rows[0]?.datePolled ? { datePolled: rows[0].datePolled } : null
    }
    logger.warn(`${url}: the API records are stored neither in MongoDB nor in PostgreSQL, every incremental poll starts from the beginning`)
    return null
}

function getValueFromParam(obj, param) {//item[api.batch.param]
    if (Array.isArray(param)) {
        let value = obj
        for (const p of param) {
            value = value[p]
        }
        return value
    }
    return obj[param]
}

// Incremental polls: every record (an object, or each object of an array) gets the dates of the request that returned
// it - the incrementalParams.startDateParam / endDateParam keys, with the values sent as query params - unless it
// already has those keys. A date not sent (e.g. no start date on a first poll without startDateValue) is not added.
function addRequestDates(items, api, params) {
    const keys = [api.incrementalParams?.startDateParam, api.incrementalParams?.endDateParam]
        .filter(key => key && params?.[key] !== undefined)
    for (const item of items)
        if (item && typeof item === "object" && !Array.isArray(item))
            for (const key of keys)
                if (!(key in item))
                    item[key] = params[key]
    return items
}

const isNotFound = error => Number(error?.response?.status) === 404

// The windows answered 404 by the previous polls, asked again first (incrementalParams.retryNotFound; only when the
// poll runs, not on the days already polled)
const missedToRetry = (api, state, windows) =>
    api.incrementalParams?.retryNotFound === true && windows.length ? (state?.missed || []).map(missed => missed.params) : []

// One window of an incremental poll (not paginated): its records stored, then no longer pending (the window is done).
// false: the API answered 404, taken as no records.
async function pollWindow(api, url, headers, window, { currentDate, collector, logName, sqlName, lastRecordDate }) {
    logger.info(`Polling ${logName} for records after ${lastRecordDate?.toISOString()}...`, window)
    const params = { ...api.queryParams, ...window }
    let response
    try {
        response = await apiRequest(api, headers, headers => axios.get(url, { headers, params }))
    }
    catch (error) {
        if (!isNotFound(error))
            throw error
        logger.info(`${logName}: 404 for ${JSON.stringify(window)}, taken as no records`)
        return false
    }
    if (!Array.isArray(response.data))
        response.data = [response.data]
    addRequestDates(response.data, api, params)
    if (response.data.length > 0) {
        logger.info(`Data from ${logName}:`, response.data.length)
        const insertingData = response.data.map(item => makeItem({ ...item, datePolled: currentDate }, url))
        const stored = await storeRecords(insertingData, url, { collector, logName, sqlName })
        await pollStatus.markPending(url, currentDate, stored)
    }
    else
        logger.info(`No new records found for ${logName} (last record date: ${lastRecordDate?.toISOString()})`)
    await collector.flush()
    await pollStatus.completePoll(url, currentDate)
    return true
}

// One incremental poll of `url`: an API, or one element of a batch API (the url with {batch} replaced: every element
// has its own state, a new element starts from startDateValue). The records of a window left incomplete by the
// previous poll are deleted first. Then, with retryNotFound, the windows that answered 404 before; then one request
// per window, from the end of the last window stored (utils/incrementalState.js) to now. Every window is saved as soon
// as it is done (a 404 counts as done, with no records): if a request fails, the next poll resumes from that window.
async function pollIncrementalUrl(api, url, headers, { sqlName, logName = `${api.name} API` } = {}) {
    const collector = createCollector(url, "api")
    await pollStatus.rollbackPending(url, { logName })
    const currentDate = new Date()
    const state = await incrementalState.get(url)
    const { lastRecordDate, windows, ends } = prepareIncremental(await lastPolledRecord(url), api, currentDate, state)
    const context = { currentDate, collector, logName, sqlName, lastRecordDate }
    for (const window of missedToRetry(api, state, windows))
        if (await pollWindow(api, url, headers, window, context))
            await incrementalState.removeMissed(url, window)
    for (const [k, window] of windows.entries()) {
        const found = await pollWindow(api, url, headers, window, context)
        if (!found && api.incrementalParams.retryNotFound === true)
            await incrementalState.addMissed(url, window)
        await incrementalState.windowDone(url, ends[k], k == windows.length - 1)
    }
}

function logPollError(label, error) {
    logger.error(`Error polling ${label}:`)
    if (error.response) {
        logger.error("Status:", error.response.status)
        logger.error("Data:", error.response.data)
        logger.error("Headers:", error.request.headers)
        logger.debug(error)
    }
    else
        logger.error(error)
}

// The headers of the requests of an API; the bearer tokens got from their authUrl (cached until they expire).
// renew: the tokens that come from an authUrl are asked again (after a 401)
async function apiHeaders(api, { renew = false } = {}) {
    if (renew)
        for (const header in api.headers)
            if (api.headers[header]?.type === "bearerToken" && api.headers[header].authUrl?.value) {
                delete tokens[api.headers[header].authUrl.value]
                delete api.headers[header].value
                delete api.headers[header].expiry
            }
    const headers = {}
    for (const header in api.headers)
        if (api.headers[header].type === "bearerToken") {
            if (!api.headers[header].value || (api.headers[header].expiry && new Date() > new Date(api.headers[header].expiry))) {
                if (tokens[api.headers[header].authUrl.value] && tokens[api.headers[header].authUrl.value].expiry && new Date() < new Date(tokens[api.headers[header].authUrl.value].expiry)) {
                    api.headers[header].value = tokens[api.headers[header].authUrl.value].token
                    if (tokens[api.headers[header].authUrl.value].expiry)
                        api.headers[header].expiry = tokens[api.headers[header].authUrl.value].expiry
                    api.bearerPosition = header
                    //headers[header] = api.headers[header].value
                    continue
                }
                logger.info(api.headers[header])
                const response = await getToken(api.headers[header].authUrl.value, api.headers[header].authUrl.requestType, api.headers[header].credentials, api.headers[header].authProfile, api.headers[header].body, api.headers[header].headers)
                logger.info("Token got", response?.data || "No response")
                api.headers[header].value = "Bearer " + response.data.access_token
                //headers[header] = api.headers[header].value
                tokens[api.headers[header].authUrl.value] = {
                    token: api.headers[header].value,
                    expiry: response.data.expires_in ? new Date(new Date().getTime() + response.data.expires_in * 1000) : null
                }
                if (response.data.expires_in)
                    api.headers[header].expiry = new Date(new Date().getTime() + response.data.expires_in * 1000)
            }
            api.bearerPosition = header
        }
    /*else if (api.headers[header].type === "basic" && api.headers[header].alwaysSend)
        headers[header] = setBasicAuthHeader(api.headers[header].credentials)*/
    if (api.bearerPosition)
        headers[api.bearerPosition] = api.headers[api.bearerPosition].value

    if (api.headers?.Authorization && api.headers.Authorization.type === "basic" && api.headers.Authorization.alwaysSend)
        headers.Authorization = setBasicAuthHeader(api.headers.Authorization.credentials)
    return headers
}

// A token that can be renewed: from an authUrl (a hardcoded one cannot)
const renewableToken = api => Object.values(api.headers || {}).some(header => header?.type === "bearerToken" && header.authUrl?.value)

// A request of an API (send(headers)): on a 401 the token is renewed once and the request repeated, so the poll goes
// on; `headers` is updated in place (the next requests of the poll use the new token). A second 401, or a token that
// cannot be renewed: the error, as before.
async function apiRequest(api, headers, send) {
    try {
        return await send(headers)
    }
    catch (error) {
        if (Number(error?.response?.status) !== 401 || !renewableToken(api))
            throw error
        logger.warn(`${api.name} API: 401, renewing the token and repeating the request`)
        Object.assign(headers, await apiHeaders(api, { renew: true }))
        return await send(headers)
    }
}

// One poll of `apis` (default: every API of apiConnectorConfig.apiUrls; utils/resetIncremental.js passes some)
async function pollAPI(apis = config.apiConnectorConfig.apiUrls) {
    if (locks.polling || locks.rebuilding) {
        logger.info(locks.polling ? "Previous API poll still running, skipping this one" : "Rebuild from sources running, skipping this API poll")
        return
    }
    locks.polling = true
    try {
        for (const api of apis || []) {
            try {
                const headers = await apiHeaders(api)

                logger.debug(api)
                if (api.batch) {
                    logger.info(`Polling ${api.name} API for batch values from ${api.batch.from}...`)
                    const batchResponse = await apiRequest(api, headers, headers => axios.get(api.batch.from, { headers }))
                    let batchValues
                    if (api.batch.pick)
                        batchValues = batchResponse.data[api.batch.pick].map(item => getValueFromParam(item, api.batch.param))
                    else
                        batchValues = batchResponse.data.map(item => getValueFromParam(item, api.batch.param))
                    logger.info(`Batch values for ${api.name} API:`, batchValues)
                    for (const batchValue of batchValues) {
                        logger.info(`Polling ${api.name} API for batch value:`, batchValue)
                        const batchUrl = api.url.replace("{batch}", batchValue)
                        if (api.incremental) {
                            // the dates (and maxWindow windows) on the request of each element, from its own last poll
                            const label = `${api.name} API (batch ${batchValue})`
                            try {
                                await pollIncrementalUrl(api, batchUrl, headers, { sqlName: batchValue, logName: label })
                            }
                            catch (error) {
                                // this element only: the others go on, its records stay pending until its next poll
                                logPollError(label, error)
                            }
                            continue
                        }
                        const collector = createCollector(batchUrl, "api")
                        const response = await apiRequest(api, headers, headers => axios.get(batchUrl, { headers }))
                        logger.info("api ", batchUrl, "called with ", {
                            headers
                        }, { response: response?.data?.length || response.data || "no response" })
                        logger.info(`Data from ${api.name} API (batch ${batchValue}):`, response.data.length)
                        if (!Array.isArray(response.data))
                            response.data = [response.data]
                        await storeSnapshot(api, batchUrl, response.data, { collector, logName: `${api.name} API (batch ${batchValue})`, sqlName: batchValue })
                        await collector.flush()
                    }
                }
                else if (api.pagination) {
                    let queryParams = { ...api.queryParams }
                    const collector = createCollector(api.url, "api")
                    let offset = api.pagination.offsetValue?.startFrom || setStartOffset(api.pagination.offsetValue?.type)
                    queryParams[api.pagination.offsetParam] = offset
                    queryParams[api.pagination.limitParam] = api.pagination.limit
                    // records of a previous poll of this API that stopped before its last page: deleted, then polled again
                    await pollStatus.rollbackPending(api.url, { logName: api.name })
                    if (api.incremental && config.apiConnectorConfig.upsertRecords)
                        await clearOrigin(api.url, "api")
                    let response
                    const currentDate = new Date()
                    const startOffset = queryParams[api.pagination.offsetParam]
                    // the requests: incremental, one per window (none if already polled today), after the windows that
                    // answered 404 before (retryNotFound); otherwise one
                    let windows = [{}], ends = [], retried = []
                    if (api.incremental) {
                        logger.info(`Polling ${api.name} API for incremental records...`)
                        const state = await incrementalState.get(api.url)
                        const prepared = prepareIncremental(await lastPolledRecord(api.url), api, currentDate, state)
                        windows = prepared.windows
                        ends = prepared.ends
                        retried = missedToRetry(api, state, windows)
                    }
                    const jobs = [...retried.map(window => ({ window, missed: true })), ...windows.map((window, k) => ({ window, k }))]
                    const snapshotItems = [] // not incremental: every page, compared with the stored records at the end
                    for (const { window, missed, k } of jobs) {
                        // every window from its first page; the dates after: an offset that is also the start date
                        // (a cursor like "since") starts from the window
                        queryParams[api.pagination.offsetParam] = startOffset
                        Object.assign(queryParams, window)
                        let firstPage = true, notFound = false
                        do {
                            logger.info(`Polling ${api.name} API with offset ${queryParams[api.pagination.offsetParam]}...`)
                            logger.info("api ", api.url, "called with ", {
                                headers
                            })
                            try {
                                response = await apiRequest(api, headers, headers => axios.get(api.url, { headers, params: queryParams }))
                            }
                            catch (error) {
                                if (!api.incremental || !isNotFound(error))
                                    throw error
                                // 404 taken as no records: on the first page the window is empty, later no more pages
                                notFound = firstPage
                                logger.info(`${api.name} API: 404 for ${JSON.stringify(window)} at offset ${queryParams[api.pagination.offsetParam]}, taken as no records`)
                                break
                            }
                            firstPage = false
                            logger.info(`Data from ${api.name} API:`, response.data?.length)
                            if (response.data) {
                                if (!Array.isArray(response.data))
                                    response.data = [response.data]
                                if (api.incremental) // the params of this page (a cursor offset may be the start date)
                                    addRequestDates(response.data, api, queryParams)
                                if (!api.incremental)
                                    snapshotItems.push(...response.data)
                                else {
                                    const insertingData = response.data.map(item => makeItem({ ...item, datePolled: currentDate }, api.url))
                                    if (insertingData.length > 0) {
                                        const stored = await storeRecords(insertingData, api.url, { collector, logName: api.name })
                                        await pollStatus.markPending(api.url, currentDate, stored)
                                        logger.info(`Inserted ${insertingData.length} new records for ${api.name} API`)
                                    }
                                    else
                                        logger.info(`No new records to insert for ${api.name} API`)
                                }
                                if (typeof api.pagination.offsetValue === "object" && api.pagination.offsetValue.from === "response")
                                    if (response.data[0]?.[api.pagination.offsetValue.value] === undefined || response.data[0]?.[api.pagination.offsetValue.value] === null || response.data[0]?.[api.pagination.offsetValue.value] === queryParams[api.pagination.offsetParam])
                                        break
                                    else
                                        queryParams[api.pagination.offsetParam] = response.data[0]?.[api.pagination.offsetValue.value]
                                else
                                    queryParams[api.pagination.offsetParam] += api.pagination.limit
                            }
                            else
                                logger.info("No sources to insert")
                        } while (api.pagination.condition(response));
                        if (!api.incremental)
                            await storeSnapshot(api, api.url, snapshotItems, { collector, currentDate })
                        // the window is done: its records are no longer pending, the next poll starts after it
                        await collector.flush()
                        await pollStatus.completePoll(api.url, currentDate)
                        if (api.incremental)
                            if (missed) {
                                if (!notFound)
                                    await incrementalState.removeMissed(api.url, window)
                            }
                            else {
                                if (notFound && api.incrementalParams.retryNotFound === true)
                                    await incrementalState.addMissed(api.url, window)
                                await incrementalState.windowDone(api.url, ends[k], k == windows.length - 1)
                            }
                    }
                    await collector.flush()
                    // every page stored: the records of this poll are no longer pending
                    await pollStatus.completePoll(api.url, currentDate)

                }
                else if (api.incremental) {
                    logger.info(`Polling ${api.name} API for incremental records...`)
                    await pollIncrementalUrl(api, api.url, headers)
                }
                else {
                    const collector = createCollector(api.url, "api")
                    const response = await apiRequest(api, headers, headers => api.method?.toLowerCase() === "post" ? axios.post(api.url, api.body, { headers }) : axios.get(api.url, { headers }))
                    logger.info("api ", api.url, "called with ", {
                        headers
                    }, { response: response?.data?.length || response.data || "no response" })
                    logger.info(`Data from ${api.name} API:`, response.data.length)
                    if (!Array.isArray(response.data))
                        response.data = [response.data]
                    await storeSnapshot(api, api.url, response.data, { collector, dropId: true })
                    await collector.flush()
                }
            }
            catch (error) {
                logPollError(`${api.name} API`, error)
            }
        }
    }
    catch (error) {
        logger.error("Error polling API:", error)
    }
    finally {
        locks.polling = false
    }
}

setBasicAuthHeader = (credentials) => {
    return "Basic " + Buffer.from(credentials.username + ":" + credentials.password).toString('base64');
}

async function getToken(url, requestType, credentials, authProfile, body, headers) {
    if (authProfile === "basic") {
        const authorization = setBasicAuthHeader(credentials)
        headers.Authorization = authorization
        logger.debug(requestType.toLowerCase(), url, {
            headers
        })
        return await axios[requestType.toLowerCase()](url, body, {
            headers: {
                'Authorization': authorization
            }
        });
    }
    else if (authProfile == "OAuth 2.0 Client Credentials Grant") {
        return await axios.post(url,
            "client_id=" + credentials.client_id + "&client_secret=" + credentials.client_secret + "&grant_type=client_credentials",
            {
                headers: {
                    "Content-Type": "application/x-www-form-urlencoded"
                }
            }
        );
    }
    else
        throw new Error("No auth profile detected")
}

pollAPI()
setInterval(pollAPI, config.apiConnectorConfig.pollInterval)

module.exports = { pollAPI, prepareIncremental }
