const config = require('../config')
const logger = require('percocologger')
const axios = require('axios')
const { createCollector } = require('../utils/entriesStore')
const { makeItem, replaceRecords, clearOrigin, storeRecords, pgQuery, waitForPostgreInit, ensurePgTable } = require('../utils/sourceRecords')
const { collectionSettings, collectionModel } = require('../utils/collections')
const locks = require('../utils/jobLocks')
const pollStatus = require('../utils/pollStatus')
let tokens = {}

const DAY = 1000 * 60 * 60 * 24
const YMD = "YYYY-MM-DD"
const utcMidnight = date => new Date(date.toISOString().split("T")[0])

// incrementalParams.maxWindow (ms): the longest time span of one request; undefined when not set or not usable (warned).
// With YYYY-MM-DD dates it is a whole number of days, at least one.
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
    if (params.startDateFormat == YMD || params.endDateFormat == YMD) {
        const days = Math.max(1, Math.floor(maxWindow / DAY))
        if (days * DAY != maxWindow)
            logger.warn(`${api.name}: maxWindow ${maxWindow} ms with YYYY-MM-DD dates: ${days} day(s)`)
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

// The date params of an incremental poll, one object per request: from the last poll (or startDateValue) to now, in
// windows of at most incrementalParams.maxWindow if set, contiguous as two consecutive polls are (start / end logic,
// pollDelay). mustPull false (no requests): already polled today. lastRecordDate: the last poll, for the logs.
function prepareIncremental(lastRecord, api, currentDate) {
    const params = api.incrementalParams
    const pollDelay = Number.isFinite(params.pollDelay) ? params.pollDelay : 0
    const dateOf = (from, to, verse) => setDateForIncremental(from, to, verse == "start" ? params.startDateFormat : params.endDateFormat,
        pollDelay, params.startDateLogic, params.endDateLogic, verse)
    const yesterday = new Date(currentDate.getTime() - DAY)
    let from, firstStart
    if (lastRecord) {
        from = new Date(lastRecord["datePolled"])
        if (from.toISOString().split("T")[0] === currentDate.toISOString().split("T")[0]) {
            logger.info(`No new records to poll for ${api.name} API (last record date: ${from.toISOString()})`)
            return { lastRecordDate: from, mustPull: false, windows: [] }
        }
    }
    else if (params.startDateValue) {
        logger.info(`No records found for ${api.name} API, polling from configured start date...`)
        from = new Date(params.startDateValue)
        firstStart = params.startDateValue // sent as configured
    }
    else {
        logger.info(`No records found for ${api.name} API, polling all records...`)
        return { mustPull: true, windows: [params.endDateParam ? { [params.endDateParam]: dateOf(yesterday, currentDate, "end") } : {}] }
    }
    const maxWindow = maxWindowOf(api)
    const bounds = maxWindow && !isNaN(from) ?
        windowBounds(from, currentDate, maxWindow, params.startDateFormat == YMD || params.endDateFormat == YMD) :
        [isNaN(from) ? yesterday : from, currentDate]
    const windows = []
    for (let k = 0; k + 1 < bounds.length; k++) {
        const window = { [params.startDateParam]: k == 0 && firstStart !== undefined ? firstStart : dateOf(bounds[k], bounds[k + 1], "start") }
        if (params.endDateParam)
            window[params.endDateParam] = dateOf(bounds[k], bounds[k + 1], "end")
        windows.push(window)
    }
    if (windows.length > 1)
        logger.info(`${api.name}: ${windows.length} requests of at most ${maxWindow} ms (maxWindow)`)
    return { lastRecordDate: lastRecord ? from : undefined, mustPull: true, windows }
}

function setStartOffset(type) {
    if (type == "RFC 3339")
        return "1970-01-01T00:00:00Z"
    return 0
}

function dbHasSameData(obj1, obj2) {
    //logger.info("Comparing objects:", JSON.stringify(obj1), JSON.stringify(obj2), JSON.stringify(obj1) == JSON.stringify(obj2))
    return JSON.stringify(obj1) == JSON.stringify(obj2)
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

// Records already stored for an origin, without _id (to insert only the new ones): none if MongoDB is off for the API
async function storedRecords(origin) {
    if (!collectionSettings("api").toMongo)
        return []
    const existing = await Source().find({ source: origin }).lean()
    existing.forEach(item => delete item._id)
    return existing
}

// The last poll of an incremental API: its most recent record (only datePolled is read). From MongoDB, or from
// PostgreSQL when the API records are not stored in MongoDB (collections.api.toMongo false); null when there is none,
// or when the records are stored in neither (every poll then starts from startDateValue, or from the beginning).
let apiFromIndex // index on the url of the PostgreSQL rows, created once
async function lastPolledRecord(url) {
    const settings = collectionSettings("api")
    if (settings.toMongo)
        return await Source().findOne({ source: url }).sort({ datePolled: -1 }).lean()
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

                logger.debug(api)
                if (api.batch) {
                    logger.info(`Polling ${api.name} API for batch values from ${api.batch.from}...`)
                    const batchResponse = await axios.get(api.batch.from, {
                        headers
                    })
                    let batchValues
                    if (api.batch.pick)
                        batchValues = batchResponse.data[api.batch.pick].map(item => getValueFromParam(item, api.batch.param))
                    else
                        batchValues = batchResponse.data.map(item => getValueFromParam(item, api.batch.param))
                    logger.info(`Batch values for ${api.name} API:`, batchValues)
                    for (const batchValue of batchValues) {
                        logger.info(`Polling ${api.name} API for batch value:`, batchValue)
                        const batchUrl = api.url.replace("{batch}", batchValue)
                        const collector = createCollector(batchUrl, "api")
                        const response = await axios.get(batchUrl, {
                            headers
                        })
                        logger.info("api ", batchUrl, "called with ", {
                            headers
                        }, { response: response?.data?.length || response.data || "no response" })
                        logger.info(`Data from ${api.name} API (batch ${batchValue}):`, response.data.length)
                        if (!Array.isArray(response.data))
                            response.data = [response.data]
                        if (config.apiConnectorConfig.upsertRecords)
                            await replaceRecords(response.data, batchUrl, { logName: api.name, sqlName: batchValue })
                        else {
                            const existingSources = await storedRecords(batchUrl)
                            const newSources = response.data.map(item => makeItem(item, batchUrl))
                            const sourcesToInsert = newSources.filter(newItem => !existingSources.some(existingItem => dbHasSameData(existingItem, newItem)))
                            if (sourcesToInsert.length > 0) {
                                await storeRecords(sourcesToInsert, batchUrl, { collector, logName: api.name, sqlName: batchValue })
                                logger.info(`Inserted ${sourcesToInsert.length} new records for ${api.name} API (batch ${batchValue})`)
                            }
                            else
                                logger.info(`No new records to insert for ${api.name} API (batch ${batchValue})`)

                        }
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
                    if (config.apiConnectorConfig.upsertRecords)
                        await clearOrigin(api.url, "api")
                    let response
                    const currentDate = new Date()
                    const startOffset = queryParams[api.pagination.offsetParam]
                    // the requests: incremental, one per window (none if already polled today); otherwise one
                    let windows = [{}]
                    if (api.incremental) {
                        logger.info(`Polling ${api.name} API for incremental records...`)
                        windows = prepareIncremental(await lastPolledRecord(api.url), api, currentDate).windows
                    }
                    const existingSources = api.incremental ? [] : await storedRecords(api.url)
                    const known = api.incremental ? null : new Set(existingSources.map(item => JSON.stringify(item)))
                    for (const window of windows) {
                        // every window from its first page; the dates after: an offset that is also the start date
                        // (a cursor like "since") starts from the window
                        queryParams[api.pagination.offsetParam] = startOffset
                        Object.assign(queryParams, window)
                        do {
                            logger.info(`Polling ${api.name} API with offset ${queryParams[api.pagination.offsetParam]}...`)
                            logger.info("api ", api.url, "called with ", {
                                headers
                            })
                            response = queryParams ? await axios.get(api.url, { headers, params: queryParams }) : await axios.get(api.url, { headers })
                            logger.info(`Data from ${api.name} API:`, response.data?.length)
                            if (response.data) {
                                if (!Array.isArray(response.data))
                                    response.data = [response.data]
                                if (config.apiConnectorConfig.upsertRecords) {
                                    const insertingData = response.data.map(item => makeItem((api.incremental ? { ...item, datePolled: currentDate } : item), api.url))
                                    const stored = await storeRecords(insertingData, api.url, { collector, logName: api.name })
                                    await pollStatus.markPending(api.url, currentDate, stored)
                                }
                                else {
                                    const newSources = response.data.map(item => makeItem((api.incremental ? { ...item, datePolled: currentDate } : item), api.url))
                                    const sourcesToInsert = api.incremental ? newSources : newSources.filter(item => {
                                        const key = JSON.stringify(item)
                                        if (known.has(key)) return false
                                        known.add(key)
                                        return true
                                    })
                                    if (sourcesToInsert.length > 0) {
                                        const stored = await storeRecords(sourcesToInsert, api.url, { collector, logName: api.name })
                                        await pollStatus.markPending(api.url, currentDate, stored)
                                        logger.info(`Inserted ${sourcesToInsert.length} new records for ${api.name} API`)
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
                    }
                    await collector.flush()
                    // every page stored: the records of this poll are no longer pending
                    await pollStatus.completePoll(api.url, currentDate)

                }
                else if (api.incremental) {
                    logger.info(`Polling ${api.name} API for incremental records...`)
                    const collector = createCollector(api.url, "api")
                    // records of a previous poll of this API that stopped before its last request (maxWindow): deleted,
                    // then polled again
                    await pollStatus.rollbackPending(api.url, { logName: api.name })
                    const currentDate = new Date()
                    const { lastRecordDate, windows } = prepareIncremental(await lastPolledRecord(api.url), api, currentDate)
                    for (const window of windows) {
                        logger.info(`Polling ${api.name} API for records after ${lastRecordDate?.toISOString()}...`, window)
                        const response = await axios.get(api.url, {
                            headers,
                            params: { ...api.queryParams, ...window }
                        })
                        if (!Array.isArray(response.data))
                            response.data = [response.data]
                        if (response.data.length > 0) {
                            logger.info(`Data from ${api.name} API:`, response.data.length)
                            const insertingData = response.data.map(item => makeItem({ ...item, datePolled: currentDate }, api.url))
                            const stored = await storeRecords(insertingData, api.url, { collector, logName: api.name })
                            await pollStatus.markPending(api.url, currentDate, stored)
                        }
                        else
                            logger.info(`No new records found for ${api.name} API (last record date: ${lastRecordDate?.toISOString()})`)
                    }
                    await collector.flush()
                    // every request done: the records of this poll are no longer pending
                    await pollStatus.completePoll(api.url, currentDate)
                }
                else {
                    const collector = createCollector(api.url, "api")
                    const response = api.method?.toLowerCase() === "post" ? await axios.post(api.url, api.body, { headers }) : await axios.get(api.url, {
                        headers
                    })
                    logger.info("api ", api.url, "called with ", {
                        headers
                    }, { response: response?.data?.length || response.data || "no response" })
                    logger.info(`Data from ${api.name} API:`, response.data.length)
                    if (!Array.isArray(response.data))
                        response.data = [response.data]
                    if (config.apiConnectorConfig.upsertRecords)
                        await replaceRecords(response.data, api.url, { logName: api.name })
                    else {
                        const existingSources = await storedRecords(api.url)
                        response.data.forEach(item => delete item._id)
                        logger.info(`Existing records for ${api.name} API:`, existingSources.length)
                        logger.info(`New records for ${api.name} API:`, response.data.length)
                        if (!Array.isArray(response.data))
                            response.data = [response.data]
                        const newSources = response.data.map(item => makeItem(item, api.url))
                        const sourcesToInsert = newSources.filter(newItem => !existingSources.some(existingItem => dbHasSameData(existingItem, newItem)))
                        if (sourcesToInsert.length > 0) {
                            await storeRecords(sourcesToInsert, api.url, { collector, logName: api.name })
                            logger.info(`Inserted ${sourcesToInsert.length} new records for ${api.name} API`)
                        }
                        else
                            logger.info(`No new records to insert for ${api.name} API`)

                    }
                    await collector.flush()
                }
            }
            catch (error) {
                logger.error(`Error polling ${api.name} API:`)
                if (error.response) {
                    logger.error("Status:", error.response.status)
                    logger.error("Data:", error.response.data)
                    logger.error("Headers:", error.request.headers)
                    logger.debug(error)
                    //logger.error("Request keys:", Object.keys(error.request))
                    //logger.error("Request: ",error.request)
                }
                else
                    logger.error(error)
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

module.exports = { pollAPI }
