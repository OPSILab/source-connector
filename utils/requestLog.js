// Every failed axios request of the Source-Connector - an error answer (status outside 2xx) or no answer (timeout,
// connection refused, DNS) - saved in the MongoDB collection "statuslog": when, which API (by url, from
// apiConnectorConfig.apiUrls), method, url, query params, headers, body, status and response data, or the error.
// Saved as they are, credentials included (Authorization headers, token requests): the collection is as private as
// the database. Body and response data are text (JSON for objects), cut at statusLog.maxDataSize characters
// (bodyTruncated / dataTruncated, with the original length). Kept forever. Off with statusLog.enabled false.
// The requests the code expects to fail (e.g. a map not found yet) are saved too: they are failed requests.
const axios = require("axios")
const logger = require("percocologger")
const config = require("../config")
const StatusLog = require("../api/models/StatusLog")

const DEFAULT_MAX_DATA_SIZE = 10000
const pending = new Set() // saves still running (settled() waits for them: tests)
let interceptor

function settings() {
    const statusLog = config.statusLog || {}
    return {
        enabled: statusLog.enabled !== false,
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

// The statuslog document of a failed request
function record(error) {
    const request = error.config || {}
    const response = error.response
    const { maxDataSize } = settings()
    const body = asText(request.data, maxDataSize)
    const data = response ? asText(response.data, maxDataSize) : {}
    let host
    try {
        host = new URL(fullUrl(request)).host
    }
    catch { }
    const doc = {
        createdAt: new Date(),
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
        error: error.message,
        code: error.code
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

// Saves the failed request; never throws (a failed save is only logged). Field names with "$" or "." in params /
// headers can be refused by older MongoDB versions: then they are saved as JSON text.
function save(error) {
    if (!settings().enabled || !error?.config)
        return Promise.resolve()
    const doc = record(error)
    const saving = StatusLog.collection.insertOne(doc)
        .catch(() => {
            const { _id, ...retry } = doc // _id set by the refused insert
            return StatusLog.collection.insertOne({ ...retry, params: asJson(doc.params), headers: asJson(doc.headers) })
        })
        .catch(saveError => logger.error(`statuslog: the failed request to ${doc.fullUrl} was not saved`, saveError?.message || saveError))
        .finally(() => pending.delete(saving))
    pending.add(saving)
    return saving
}

// On the default axios instance (the one every module of the Source-Connector uses). The error goes on to the caller
// as it was; the save runs in the background (a slow or unavailable MongoDB does not delay the caller).
function install() {
    if (interceptor !== undefined)
        return
    interceptor = axios.interceptors.response.use(response => response, error => {
        save(error)
        return Promise.reject(error)
    })
    StatusLog.collection.createIndex({ createdAt: -1 }).catch(error => logger.warn("statuslog: index on createdAt not created", error?.message || error))
    logger.info("Failed HTTP requests saved in the statuslog collection" + (settings().enabled ? "" : " (off: statusLog.enabled false)"))
}

function uninstall() {
    if (interceptor === undefined)
        return
    axios.interceptors.response.eject(interceptor)
    interceptor = undefined
}

const settled = () => Promise.all([...pending])

module.exports = { install, uninstall, settled, record }
