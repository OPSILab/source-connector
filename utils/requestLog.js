// Every axios request of the Source-Connector saved in the MongoDB collection "statuslog": the failed ones - an error
// answer (status outside 2xx) or no answer (timeout, connection refused, DNS) - and the successful ones. When,
// outcome ("error" / "success"), which API (by url, from apiConnectorConfig.apiUrls), method, url, query params,
// headers, body, status and response data (or the error), how long it took (durationMs).
// Saved as they are, credentials included (Authorization headers, token requests): the collection is as private as
// the database. Body and response data are text (JSON for objects), cut at statusLog.maxDataSize characters
// (bodyTruncated / dataTruncated, with the original length).
// The errors are kept forever; the successes for statusLog.successRetentionDays (default 30, 0: forever: expiresAt,
// with a TTL index of MongoDB). statusLog.successes false: only the errors. Off with statusLog.enabled false.
// The requests the code expects to fail (e.g. a map not found yet) are saved too: they are failed requests.
const axios = require("axios")
const logger = require("percocologger")
const config = require("../config")
const StatusLog = require("../api/models/StatusLog")

const DEFAULT_MAX_DATA_SIZE = 10000
const pending = new Set() // saves still running (settled() waits for them: tests)
let interceptor, requestInterceptor

const DEFAULT_SUCCESS_RETENTION_DAYS = 30
const DAY = 24 * 60 * 60 * 1000

function settings() {
    const statusLog = config.statusLog || {}
    const days = Number(statusLog.successRetentionDays)
    return {
        enabled: statusLog.enabled !== false,
        successes: statusLog.successes !== false,
        successRetentionDays: statusLog.successRetentionDays === undefined || !Number.isFinite(days) || days < 0 ? DEFAULT_SUCCESS_RETENTION_DAYS : days,
        maxDataSize: Number.isInteger(statusLog.maxDataSize) && statusLog.maxDataSize > 0 ? statusLog.maxDataSize : DEFAULT_MAX_DATA_SIZE
    }
}

// { text, truncated, length } of a body / response: strings as they are, buffers as UTF-8, the rest as JSON
function asText(value, max) {
    if (value === undefined || value === null)
        return {}
    let text
    if (typeof value == "string")
        text = value
    else if (Buffer.isBuffer(value) || value instanceof ArrayBuffer)
        text = Buffer.from(value).toString("utf8")
    else
        try {
            text = JSON.stringify(value)
        }
        catch {
            text = String(value)
        }
    return text.length > max ? { text: text.slice(0, max), truncated: true, length: text.length } : { text }
}

const withoutQuery = url => typeof url == "string" ? url.split("?")[0] : undefined
const escapeRegex = text => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

// The name of the configured API a request belongs to (its url, batch url or values, token url), if any
function apiOf(url) {
    const base = withoutQuery(url)
    if (!base)
        return undefined
    for (const api of config.apiConnectorConfig?.apiUrls || []) {
        const apiBase = withoutQuery(api.url)
        if (apiBase == base || withoutQuery(api.batch?.from) == base)
            return api.name
        if (apiBase?.includes("{batch}") && new RegExp("^" + apiBase.split("{batch}").map(escapeRegex).join(".+") + "$").test(base))
            return api.name
        if (Object.values(api.headers || {}).some(header => withoutQuery(header?.authUrl?.value) == base))
            return api.name
    }
    return undefined
}

function plainHeaders(headers) {
    if (!headers)
        return undefined
    return typeof headers.toJSON == "function" ? headers.toJSON() : { ...headers }
}

function fullUrl(request) {
    try {
        return axios.getUri(request)
    }
    catch {
        return request.url
    }
}

// The statuslog document of a request: a failed one (an axios error) or a successful one (an axios response)
function record(error, success) {
    const request = (success || error).config || {}
    const response = success || error.response
    const { maxDataSize, successRetentionDays } = settings()
    const body = asText(request.data, maxDataSize)
    const data = response ? asText(response.data, maxDataSize) : {}
    let host
    try {
        host = new URL(fullUrl(request)).host
    }
    catch { }
    const createdAt = new Date()
    const doc = {
        createdAt,
        outcome: success ? "success" : "error",
        api: apiOf(request.url),
        host,
        method: request.method?.toUpperCase(),
        url: request.url,
        fullUrl: fullUrl(request),
        params: request.params,
        headers: plainHeaders(request.headers),
        body: body.text,
        status: response?.status,
        statusText: response?.statusText,
        data: data.text,
        error: success ? undefined : error.message,
        code: success ? undefined : error.code,
        durationMs: typeof request.requestLogStart == "number" ? createdAt.getTime() - request.requestLogStart : undefined,
        // the successes expire (TTL index on expiresAt), the errors are kept
        expiresAt: success && successRetentionDays > 0 ? new Date(createdAt.getTime() + successRetentionDays * DAY) : undefined
    }
    if (body.truncated)
        Object.assign(doc, { bodyTruncated: true, bodyLength: body.length })
    if (data.truncated)
        Object.assign(doc, { dataTruncated: true, dataLength: data.length })
    for (const key of Object.keys(doc))
        if (doc[key] === undefined)
            delete doc[key]
    return doc
}

const asJson = value => value === undefined ? undefined : JSON.stringify(value)

// Saves a request (error: an axios error; success: an axios response); never throws (a failed save is only logged).
// Field names with "$" or "." in params / headers can be refused by older MongoDB versions: then they are saved as
// JSON text.
function save(error, success) {
    const { enabled, successes } = settings()
    if (!enabled || !(success || error)?.config || (success && !successes))
        return Promise.resolve()
    const doc = record(error, success)
    const saving = StatusLog.collection.insertOne(doc)
        .catch(() => {
            const { _id, ...retry } = doc // _id set by the refused insert
            return StatusLog.collection.insertOne({ ...retry, params: asJson(doc.params), headers: asJson(doc.headers) })
        })
        .catch(saveError => logger.error(`statuslog: the request to ${doc.fullUrl} was not saved`, saveError?.message || saveError))
        .finally(() => pending.delete(saving))
    pending.add(saving)
    return saving
}

// On the default axios instance (the one every module of the Source-Connector uses). The response / error goes on to
// the caller as it was; the save runs in the background (a slow or unavailable MongoDB does not delay the caller).
function install() {
    if (interceptor !== undefined)
        return
    requestInterceptor = axios.interceptors.request.use(request => {
        request.requestLogStart = Date.now()
        return request
    })
    interceptor = axios.interceptors.response.use(response => {
        save(undefined, response)
        return response
    }, error => {
        save(error)
        return Promise.reject(error)
    })
    StatusLog.collection.createIndex({ createdAt: -1 }).catch(error => logger.warn("statuslog: index on createdAt not created", error?.message || error))
    // only the documents with expiresAt (the successes) expire
    StatusLog.collection.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }).catch(error => logger.warn("statuslog: TTL index on expiresAt not created", error?.message || error))
    const { enabled, successes, successRetentionDays } = settings()
    logger.info(!enabled ? "HTTP requests not saved (statusLog.enabled false)" :
        "HTTP requests saved in the statuslog collection: the errors" + (successes ? `, the successes${successRetentionDays ? " for " + successRetentionDays + " days" : ""}` : ""))
}

function uninstall() {
    if (interceptor === undefined)
        return
    axios.interceptors.response.eject(interceptor)
    axios.interceptors.request.eject(requestInterceptor)
    interceptor = requestInterceptor = undefined
}

const settled = () => Promise.all([...pending])

module.exports = { install, uninstall, settled, record }
