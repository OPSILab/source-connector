// One poll of every API in config.js (apiConnectorConfig.apiUrls), against the real APIs, writing every request
// (method, url, query params, body) and its response (status, data) to a JSON file. Nothing is read from or written to
// the databases: the incremental APIs see a last poll of yesterday (LAST_POLL to change it), the non incremental ones
// no stored record; storing, pollStatus and keys/values/entries are no-ops.
//
//   node test/manual/apiRequestLog.js [output file]      (default: test/output/api-requests-<timestamp>.json)
//
//   LAST_POLL=2026-10-20T08:00:00Z   date of the simulated last poll of the incremental APIs (default: 24 hours ago)
//   LOG_LEVEL=debug                  connector logs on the console (default: info; never saved to logs/).
//                                    debug prints the api configurations, credentials included.
//
// Not a *.test.js file: `npm test` does not run it. Everything is written as it is, the requests to the token
// endpoints too (credentials in the body, access_token in the data) and the query params (api keys): the file stays
// on this machine (test/output/ is in .gitignore).
const path = require("path")
const fs = require("fs")
const os = require("os")
const Module = require("module")

const ROOT = path.resolve(__dirname, "../..")

process.percocologger = {
    ...require(path.join(ROOT, "percocologger.config.json")),
    logLevel: process.env.LOG_LEVEL || "info",
    saveLog: false,
    logPath: path.join(os.tmpdir(), "source-connector-apiRequestLog") + path.sep // the logger creates its file anyway
}
process.env.LEVEL = process.percocologger.logLevel

// Replaces a project module before apiConnector requires it
function stub(relPath, exports) {
    const file = require.resolve(path.join(ROOT, relPath))
    const mod = new Module(file)
    mod.filename = file
    mod.loaded = true
    mod.exports = exports
    require.cache[file] = mod
}

const DAY = 1000 * 60 * 60 * 24
const lastPoll = process.env.LAST_POLL ? new Date(process.env.LAST_POLL) : new Date(Date.now() - DAY)
if (isNaN(lastPoll))
    throw new Error(`LAST_POLL "${process.env.LAST_POLL}" is not a date`)

// The collection of the API records: the last poll (incremental APIs) is `lastPoll`, nothing stored
const query = result => ({ sort: () => query(result), lean: async () => result })
stub("utils/collections.js", {
    collectionSettings: () => ({ toMongo: true, toPostgres: false }),
    collectionModel: () => ({
        findOne: () => query({ datePolled: lastPoll }),
        find: () => query([])
    })
})
stub("utils/sourceRecords.js", {
    makeItem: (item, origin) => ({ ...item, source: origin }),
    replaceRecords: async () => { },
    clearOrigin: async () => { },
    storeRecords: async () => ({ mongo: [], postgres: [] }),
    pgQuery: async () => ({ rows: [] }),
    waitForPostgreInit: async () => { },
    ensurePgTable: async () => { }
})
stub("utils/pollStatus.js", {
    rollbackPending: async () => ({ mongo: 0, postgres: 0 }),
    markPending: async () => { },
    completePoll: async () => { }
})
stub("utils/entriesStore.js", {
    createCollector: () => ({ add() { }, flush: async () => { } })
})

const config = require(path.join(ROOT, "config.js"))
const axios = require("axios")
const locks = require(path.join(ROOT, "utils/jobLocks.js"))

const apis = config.apiConnectorConfig?.apiUrls || []
const authUrls = new Set(apis.flatMap(api => Object.values(api.headers || {}).map(header => header?.authUrl?.value).filter(Boolean)))
const withoutQuery = url => String(url).split("?")[0]

// The api of a request: by url (batch urls with their {batch} value) and, among the APIs with the same url, by method;
// token endpoints by authUrl
const escape = text => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
function apiOf(url, method) {
    const base = withoutQuery(url)
    const same = apis.filter(api => withoutQuery(api.url) == base || withoutQuery(api.batch?.from) == base ||
        (api.url?.includes("{batch}") && new RegExp("^" + withoutQuery(api.url).split("{batch}").map(escape).join(".+") + "$").test(base)))
    const api = same.find(api => (api.method || "get").toLowerCase() == method?.toLowerCase()) || same[0]
    return api?.name || (authUrls.has(url) ? "token" : undefined)
}

const output = path.resolve(process.argv[2] || path.join(ROOT, "test/output", `api-requests-${new Date().toISOString().replace(/[:.]/g, "-")}.json`))
fs.mkdirSync(path.dirname(output), { recursive: true })
fs.writeFileSync(output, "[\n")
let count = 0

function write(entry) {
    fs.appendFileSync(output, (count++ ? ",\n" : "") + JSON.stringify(entry, null, 2))
}

let sequence = 0
axios.interceptors.request.use(request => {
    const url = axios.getUri({ ...request, params: undefined })
    request.requestLog = {
        n: ++sequence,
        api: apiOf(request.url, request.method),
        at: new Date().toISOString(),
        request: {
            method: request.method?.toUpperCase(),
            url,
            params: request.params,
            fullUrl: axios.getUri(request),
            body: request.data
        }
    }
    return request
})
axios.interceptors.response.use(
    response => {
        const log = response.config.requestLog
        if (log)
            write({ ...log, response: { status: response.status, data: response.data } })
        return response
    },
    error => {
        const log = error.config?.requestLog
        if (log)
            write({
                ...log,
                response: error.response ?
                    { status: error.response.status, data: error.response.data } :
                    { error: error.message }
            })
        return Promise.reject(error)
    }
)

console.log(`Last poll of the incremental APIs: ${lastPoll.toISOString()}`)
console.log(`${apis.length} APIs: ${apis.map(api => api.name).join(", ")}`)

// apiConnector polls once as soon as it is loaded; its timer is not started
const realSetInterval = global.setInterval
global.setInterval = () => ({ unref() { } })
try {
    require(path.join(ROOT, "inputConnectors/apiConnector.js"))
}
finally {
    global.setInterval = realSetInterval
}

const finished = setInterval(() => {
    if (locks.polling)
        return
    clearInterval(finished)
    fs.appendFileSync(output, "\n]\n")
    console.log(`${count} requests written to ${output}`)
    process.exit(0)
}, 200)
