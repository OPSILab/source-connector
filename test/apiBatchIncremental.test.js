// Incremental batch APIs and the request dates on the records of every incremental poll (apiConnector): the list of batch.from as it is, the dates (and maxWindow windows) on the
// request of each element (api.url with {batch} replaced), each element from its own last poll; an element that fails
// does not stop the others. Every window is saved as soon as it is done: an interrupted poll resumes from the window
// that failed; a 404 is taken as no records (with retryNotFound, asked again at the next polls); a 401 renews the
// token and repeats the request. The records, the
// pending polls, the state of the polls and the keys / values / entries are replaced by in-memory stubs: no database.
const { stub, load, config, resetConfig } = require("./helpers/env")
const { test, describe, before, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")

// records: { _id, source (the url of the element), datePolled, ... }
let records = [], pending = [], nextId = 1
const matches = (doc, filter) => Object.entries(filter).every(([key, value]) => doc[key] === value)
const query = docs => ({
    sort: spec => query([...docs].sort((a, b) => Object.entries(spec).reduce((r, [k, dir]) => r || (a[k] < b[k] ? -dir : a[k] > b[k] ? dir : 0), 0))),
    lean: async () => structuredClone(docs)
})
const model = {
    find: filter => query(records.filter(doc => matches(doc, filter))),
    findOne: filter => {
        const q = query(records.filter(doc => matches(doc, filter)))
        return { sort: spec => ({ lean: async () => (await q.sort(spec).lean())[0] || null }), lean: async () => (await q.lean())[0] || null }
    }
}
stub("utils/collections.js", {
    collectionSettings: () => ({ toMongo: true, toPostgres: false, originField: "source", mongo: "sources" }),
    collectionModel: () => model
})
stub("utils/entriesStore.js", { createCollector: () => ({ add: async () => { }, flush: async () => { } }) })
stub("utils/sourceRecords.js", {
    makeItem: (item, origin) => ({ ...item, source: origin }),
    storeRecords: async docs => {
        const ids = docs.map(doc => { const _id = nextId++; records.push({ _id, ...structuredClone(doc) }); return _id })
        return { mongo: ids, postgres: [] }
    },
    replaceRecords: async () => { throw new Error("replaceRecords not expected") },
    clearOrigin: async () => { }, pgQuery: async () => ({ rows: [] }), waitForPostgreInit: async () => { }, ensurePgTable: async () => { }
})
stub("utils/pollStatus.js", {
    markPending: async (url, datePolled, { mongo = [] } = {}) => { pending.push(...mongo.map(_id => ({ _id, url, datePolled }))) },
    completePoll: async (url, datePolled) => { pending = pending.filter(p => !(p.url == url && p.datePolled == datePolled)) },
    rollbackPending: async url => {
        const ids = new Set(pending.filter(p => p.url == url).map(p => p._id))
        records = records.filter(doc => !ids.has(doc._id))
        pending = pending.filter(p => p.url != url)
        return { mongo: ids.size, postgres: 0 }
    }
})

// the state of the polls of each url (utils/incrementalState.js)
let states = {}
const sameParams = (a, b) => JSON.stringify(a) === JSON.stringify(b)
stub("utils/incrementalState.js", {
    get: async url => states[url] ? structuredClone(states[url]) : null,
    windowDone: async (url, end, last) => { states[url] = { ...states[url], lastEnd: new Date(end), complete: !!last } },
    addMissed: async (url, params) => {
        const state = states[url] ||= {}
        state.missed ||= []
        if (!state.missed.some(missed => sameParams(missed.params, params)))
            state.missed.push({ params, at: new Date() })
    },
    removeMissed: async (url, params) => { if (states[url]?.missed) states[url].missed = states[url].missed.filter(missed => !sameParams(missed.params, params)) },
    clear: async () => { }, urlsMatching: async () => []
})

const DAY = 1000 * 60 * 60 * 24
const ymd = time => new Date(time).toISOString().split("T")[0]
const daysAgo = days => ymd(Date.now() - days * DAY)
const LIST = "https://api.example/stations"
const ITEM = id => `https://api.example/stations/${id}/readings`

let pollAPI, requests, elements, failing, notFound, respond
const realGet = axios.get

before(async () => {
    config.apiConnectorConfig.apiUrls = []
    const realSetInterval = global.setInterval
    global.setInterval = (...args) => realSetInterval(...args).unref()
    try {
        pollAPI = load("inputConnectors/apiConnector.js").pollAPI
    }
    finally {
        global.setInterval = realSetInterval
    }
    const locks = load("utils/jobLocks.js")
    while (locks.polling)
        await new Promise(resolve => setTimeout(resolve, 10))
})

beforeEach(() => {
    resetConfig()
    records = []
    pending = []
    states = {}
    requests = []
    elements = ["a", "b"]
    failing = {}
    notFound = {}
    axios.get = async (url, options) => {
        requests.push({ url, params: structuredClone(options?.params) })
        if (url == LIST)
            return { data: { items: elements.map(id => ({ station: { code: id } })) } }
        const id = url.split("/")[4]
        const n = requests.filter(r => r.url == url).length
        if (failing[id]?.includes(n))
            throw Object.assign(new Error("Request failed with status code 422"), { response: { status: 422, data: { detail: "bad dates" } }, request: { headers: {} } })
        if (notFound[id]?.includes(n))
            throw Object.assign(new Error("Request failed with status code 404"), { response: { status: 404, data: "Not Found" }, request: { headers: {} } })
        return respond(url, options, id)
    }
    respond = (url, options, id) => ({ data: [{ reading: `${id}-${options?.params?.from}` }] })
})
afterEach(() => { axios.get = realGet })

const batchApi = (extra = {}) => ({
    name: "Stations",
    url: "https://api.example/stations/{batch}/readings",
    batch: { from: LIST, pick: "items", param: ["station", "code"] },
    incremental: true,
    incrementalParams: {
        startDateParam: "from", startDateFormat: "YYYY-MM-DD", startDateValue: daysAgo(20), startDateLogic: "inclusive",
        endDateParam: "to", endDateFormat: "YYYY-MM-DD", endDateLogic: "inclusive", maxWindow: 10 * DAY
    },
    ...extra
})
const windowsOf = url => requests.filter(r => r.url == url).map(r => [r.params.from, r.params.to])

describe("incremental batch API", () => {
    test("the list without dates; every element in windows from startDateValue, its records under its own url", async () => {
        config.apiConnectorConfig.apiUrls = [batchApi()]
        await pollAPI()
        assert.deepEqual(requests.filter(r => r.url == LIST).map(r => r.params), [undefined])
        for (const id of ["a", "b"])
            assert.deepEqual(windowsOf(ITEM(id)), [[daysAgo(20), daysAgo(11)], [daysAgo(10), daysAgo(1)]])
        assert.deepEqual(records.map(doc => [doc.source, doc.reading]), [
            [ITEM("a"), `a-${daysAgo(20)}`], [ITEM("a"), `a-${daysAgo(10)}`],
            [ITEM("b"), `b-${daysAgo(20)}`], [ITEM("b"), `b-${daysAgo(10)}`]
        ])
        assert.ok(records.every(doc => doc.datePolled instanceof Date))
        assert.deepEqual(pending, []) // every element complete
    })

    test("every element from its own last poll: a new element from startDateValue, a polled one skipped today", async () => {
        config.apiConnectorConfig.apiUrls = [batchApi()]
        await pollAPI()
        elements = ["a", "b", "c"]
        requests = []
        await pollAPI()
        assert.deepEqual(windowsOf(ITEM("a")), []) // already polled today
        assert.deepEqual(windowsOf(ITEM("b")), [])
        assert.deepEqual(windowsOf(ITEM("c")), [[daysAgo(20), daysAgo(11)], [daysAgo(10), daysAgo(1)]])
    })

    test("an element that fails: the others go on; its first window kept, its next poll (same day) resumes from the failed one", async () => {
        failing = { a: [2] } // the second window of "a"
        config.apiConnectorConfig.apiUrls = [batchApi()]
        await pollAPI()
        assert.deepEqual(windowsOf(ITEM("b")).length, 2) // "b" polled anyway
        assert.deepEqual(records.filter(doc => doc.source == ITEM("a")).map(doc => doc.reading), [`a-${daysAgo(20)}`])
        assert.deepEqual(pending, []) // the first window of "a" is done
        assert.equal(states[ITEM("a")].complete, false)

        failing = {}
        requests = []
        await pollAPI()
        // "a": only the window that failed; "b" complete: skipped
        assert.deepEqual(windowsOf(ITEM("a")), [[daysAgo(10), daysAgo(1)]])
        assert.deepEqual(windowsOf(ITEM("b")), [])
        assert.deepEqual(records.filter(doc => doc.source == ITEM("a")).map(doc => doc.reading), [`a-${daysAgo(20)}`, `a-${daysAgo(10)}`])
        assert.equal(states[ITEM("a")].complete, true)
    })

    test("not incremental: the element requests without dates, as before", async () => {
        config.apiConnectorConfig.apiUrls = [batchApi({ incremental: false })]
        await pollAPI()
        assert.deepEqual(requests.filter(r => r.url != LIST).map(r => [r.url, r.params]), [[ITEM("a"), undefined], [ITEM("b"), undefined]])
    })
})

describe("the request dates on the records of an incremental poll (incrementalParams.startDateParam / endDateParam)", () => {
    test("each object of an array gets the dates of its request; keys already there kept; not objects as they are", async () => {
        elements = ["a"]
        respond = () => ({ data: [{ reading: 1 }, { reading: 2, from: "own", to: null }, "plain"] })
        config.apiConnectorConfig.apiUrls = [batchApi()]
        await pollAPI()
        assert.deepEqual(records.map(({ reading, from, to }) => [reading, from, to]), [
            [1, daysAgo(20), daysAgo(11)], [2, "own", null], [undefined, undefined, undefined],
            [1, daysAgo(10), daysAgo(1)], [2, "own", null], [undefined, undefined, undefined]
        ])
    })

    test("an object (not an array) gets them too", async () => {
        elements = ["a"]
        respond = () => ({ data: { reading: 1 } })
        config.apiConnectorConfig.apiUrls = [batchApi({ incrementalParams: { ...batchApi().incrementalParams, maxWindow: undefined } })]
        await pollAPI()
        assert.deepEqual(records.map(({ reading, from, to }) => [reading, from, to]), [[1, daysAgo(20), daysAgo(1)]])
    })

    test("paginated incremental API: the dates of the window on every page", async () => {
        const URL_P = "https://api.example/paged"
        axios.get = async (url, options) => {
            requests.push({ url, params: structuredClone(options?.params) })
            return { data: options.params.offset == 0 ? [{ n: options.params.from }] : [] }
        }
        config.apiConnectorConfig.apiUrls = [{
            name: "Paged", url: URL_P, incremental: true, incrementalParams: batchApi().incrementalParams,
            pagination: { offsetParam: "offset", limitParam: "limit", limit: 2, condition: response => response.data.length > 0 }
        }]
        await pollAPI()
        assert.deepEqual(records.map(({ n, from, to }) => [n, from, to]), [[daysAgo(20), daysAgo(20), daysAgo(11)], [daysAgo(10), daysAgo(10), daysAgo(1)]])
    })

    test("not incremental: nothing added", async () => {
        elements = ["a"]
        config.apiConnectorConfig.apiUrls = [batchApi({ incremental: false })]
        await pollAPI()
        assert.deepEqual(records.map(doc => [doc.from, doc.to]), [[undefined, undefined]])
    })
})

describe("windows saved one by one (maxWindow) and 404", () => {
    const daily = (extra = {}) => batchApi({ incrementalParams: { ...batchApi().incrementalParams, maxWindow: DAY, startDateValue: daysAgo(5), ...extra } })
    const days = (from, to) => Array.from({ length: from - to + 1 }, (_, i) => daysAgo(from - i))

    test("interrupted at the third of five windows: the first two kept and never asked again", async () => {
        elements = ["a"]
        failing = { a: [3] }
        config.apiConnectorConfig.apiUrls = [daily()]
        await pollAPI()
        assert.deepEqual(records.map(doc => doc.reading), [`a-${daysAgo(5)}`, `a-${daysAgo(4)}`])
        failing = {}
        requests = []
        await pollAPI()
        assert.deepEqual(windowsOf(ITEM("a")).map(([from]) => from), days(3, 1))
        assert.deepEqual(records.map(doc => doc.reading), days(5, 1).map(day => `a-${day}`))
    })

    test("404: no records, the poll goes on; not asked again by default", async () => {
        elements = ["a"]
        notFound = { a: [2] } // the window of 4 days ago
        config.apiConnectorConfig.apiUrls = [daily()]
        await pollAPI()
        assert.deepEqual(windowsOf(ITEM("a")).map(([from]) => from), days(5, 1))
        assert.deepEqual(records.map(doc => doc.reading), days(5, 1).filter(day => day != daysAgo(4)).map(day => `a-${day}`))
        assert.equal(states[ITEM("a")].complete, true)
        assert.equal(states[ITEM("a")].missed, undefined)
        // the next day: from where the poll ended, the 404 window not asked again
        states[ITEM("a")].lastEnd = new Date(Date.now() - DAY)
        requests = []
        await pollAPI()
        assert.ok(!windowsOf(ITEM("a")).some(([from]) => from == daysAgo(4)))
    })

    test("retryNotFound: the 404 windows asked again first at the next polls, until they answer", async () => {
        elements = ["a"]
        notFound = { a: [2] }
        config.apiConnectorConfig.apiUrls = [daily({ retryNotFound: true })]
        await pollAPI()
        assert.deepEqual(states[ITEM("a")].missed.map(missed => missed.params.from), [daysAgo(4)])
        // the same day: the poll is skipped, the missed window too
        requests = []
        await pollAPI()
        assert.deepEqual(windowsOf(ITEM("a")), [])
        // the next day (lastEnd moved back a day): the missed window first, still 404; then the new day
        states[ITEM("a")].lastEnd = new Date(Date.now() - DAY)
        notFound = { a: [1] }
        requests = []
        await pollAPI()
        assert.deepEqual(windowsOf(ITEM("a")).map(([from]) => from), [daysAgo(4), daysAgo(1)])
        assert.deepEqual(states[ITEM("a")].missed.map(missed => missed.params.from), [daysAgo(4)])
        // the day after: it answers, it is no longer missed
        states[ITEM("a")].lastEnd = new Date(Date.now() - DAY)
        notFound = {}
        requests = []
        await pollAPI()
        assert.deepEqual(windowsOf(ITEM("a"))[0][0], daysAgo(4))
        assert.ok(records.some(doc => doc.reading == `a-${daysAgo(4)}`))
        assert.deepEqual(states[ITEM("a")].missed, [])
    })

    test("paginated: a page that fails deletes only the pages of its window; a 404 after the first page ends the window", async () => {
        const URL_P = "https://api.example/paged"
        let calls = 0, fail = true
        axios.get = async (url, options) => {
            calls++
            requests.push({ url, params: structuredClone(options?.params) })
            const { offset, from } = options.params
            if (fail && from == daysAgo(3) && offset == 2)
                throw Object.assign(new Error("Request failed with status code 401"), { response: { status: 401 }, request: { headers: {} } })
            if (offset == 4) // no more pages: 404
                throw Object.assign(new Error("Request failed with status code 404"), { response: { status: 404 }, request: { headers: {} } })
            return { data: [{ page: offset, day: from }] }
        }
        config.apiConnectorConfig.apiUrls = [{
            name: "Paged", url: URL_P, incremental: true, incrementalParams: daily().incrementalParams,
            pagination: { offsetParam: "offset", limitParam: "limit", limit: 2, condition: response => response.data.length > 0 }
        }]
        await pollAPI()
        // the windows of 5 and 4 days ago done (pages 0 and 2, then 404); 3 days ago: its first page stored, then 401
        assert.deepEqual(records.map(doc => `${doc.day}/${doc.page}`), [`${daysAgo(5)}/0`, `${daysAgo(5)}/2`, `${daysAgo(4)}/0`, `${daysAgo(4)}/2`, `${daysAgo(3)}/0`])
        assert.equal(pending.length, 1)
        fail = false
        requests = []
        await pollAPI()
        // its page 0 deleted, the window asked again from its first page; the windows before never asked again
        assert.deepEqual(requests.filter(r => !r.params.offset).map(r => r.params.from), days(3, 1))
        assert.deepEqual(records.filter(doc => doc.day == daysAgo(3)).map(doc => doc.page), [0, 2])
        assert.equal(records.length, 10)
        assert.equal(states[URL_P].complete, true)
        assert.equal(states[URL_P].missed, undefined) // a 404 after the first page is not a missed window
    })
})

describe("401: the token renewed and the request repeated, the poll goes on", () => {
    const realPost = axios.post
    let issued
    beforeEach(() => {
        issued = 0
        axios.post = async () => ({ data: { access_token: "tok-" + (++issued), expires_in: 3600 } })
    })
    afterEach(() => { axios.post = realPost })

    const withToken = (authUrl, extra = {}) => batchApi({
        incrementalParams: { ...batchApi().incrementalParams, maxWindow: DAY, startDateValue: daysAgo(3) },
        headers: {
            Authorization: { type: "bearerToken", authProfile: "OAuth 2.0 Client Credentials Grant", credentials: { client_id: "c", client_secret: "s" }, authUrl: { value: authUrl, requestType: "POST" } }
        },
        ...extra
    })
    // the first token expires at the second window: 401 for it
    const expiring = () => {
        respond = (url, options, id) => {
            if (options.headers.Authorization == "Bearer tok-1" && options.params.from == daysAgo(2))
                throw Object.assign(new Error("Request failed with status code 401"), { response: { status: 401 }, request: { headers: {} } })
            return { data: [{ reading: `${id}-${options.params.from}`, token: options.headers.Authorization }] }
        }
    }

    test("a token from authUrl: renewed once, every window in the same poll", async () => {
        elements = ["a"]
        expiring()
        config.apiConnectorConfig.apiUrls = [withToken("https://auth.example/token-1")]
        await pollAPI()
        assert.equal(issued, 2)
        assert.deepEqual(records.map(doc => [doc.reading, doc.token]), [
            [`a-${daysAgo(3)}`, "Bearer tok-1"], [`a-${daysAgo(2)}`, "Bearer tok-2"], [`a-${daysAgo(1)}`, "Bearer tok-2"]
        ])
        assert.equal(states[ITEM("a")].complete, true)
    })

    test("still 401 with the new token: the poll stops, the next one resumes from that window", async () => {
        elements = ["a"]
        respond = (url, options, id) => {
            if (options.params.from == daysAgo(2))
                throw Object.assign(new Error("Request failed with status code 401"), { response: { status: 401 }, request: { headers: {} } })
            return { data: [{ reading: `${id}-${options.params.from}` }] }
        }
        config.apiConnectorConfig.apiUrls = [withToken("https://auth.example/token-2")]
        await pollAPI()
        assert.equal(issued, 2)
        assert.deepEqual(records.map(doc => doc.reading), [`a-${daysAgo(3)}`])
        assert.equal(states[ITEM("a")].complete, false)
    })

    test("a hardcoded token (no authUrl): not renewed, the poll stops at the 401", async () => {
        elements = ["a"]
        respond = (url, options, id) => {
            if (options.params.from == daysAgo(2))
                throw Object.assign(new Error("Request failed with status code 401"), { response: { status: 401 }, request: { headers: {} } })
            return { data: [{ reading: `${id}-${options.params.from}` }] }
        }
        config.apiConnectorConfig.apiUrls = [withToken(undefined, { headers: { "X-API-Key": { type: "bearerToken", authProfile: "hardcoded", value: "key" } } })]
        await pollAPI()
        assert.equal(issued, 0)
        assert.equal(requests.filter(r => r.url == ITEM("a")).length, 2) // not repeated
        assert.deepEqual(records.map(doc => doc.reading), [`a-${daysAgo(3)}`])
    })
})
