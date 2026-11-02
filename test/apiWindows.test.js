// Incremental polls split in windows (incrementalParams.maxWindow, ms): from the last poll (or startDateValue) to now,
// one request (paginated: one series of pages) per window of at most maxWindow, the dates of two windows following
// one another as those of two consecutive polls do. On real MongoDB + PostgreSQL (see helpers/db.js).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")
const { loadApiConnector, fakePagedApi } = require("./helpers/apiConnector")

const URL_A = "https://api.example/windows"
const DAY = 1000 * 60 * 60 * 24
const ymd = time => new Date(time).toISOString().split("T")[0]
const daysAgo = days => ymd(Date.now() - days * DAY)

let pollAPI, records, Source, Status, fake

before(async () => {
    await db.setup(__filename)
    records = load("utils/sourceRecords.js")
    Source = load("utils/collections.js").collectionModel("api")
    Status = load("api/models/Status.js")
    await load("utils/pollStatus.js").ensurePgStatusTable()
    pollAPI = await loadApiConnector()
})
after(db.teardown)
beforeEach(async () => {
    await db.clean()
    await db.query("TRUNCATE status RESTART IDENTITY")
})
afterEach(() => fake?.restore())

const pagination = { offsetParam: "offset", limitParam: "limit", limit: 2, condition: response => response.data.length > 0 }
const ymdParams = (endDateLogic, extra = {}) => ({
    startDateParam: "from", startDateFormat: "YYYY-MM-DD", startDateValue: daysAgo(30),
    endDateParam: "to", endDateFormat: "YYYY-MM-DD", startDateLogic: "inclusive", endDateLogic, maxWindow: 10 * DAY, ...extra
})
const windowsOf = requests => requests.filter(r => !r.offset).map(r => [r.params.from, r.params.to])

describe("paginated, YYYY-MM-DD, first poll from startDateValue (30 days ago), maxWindow 10 days", () => {
    test("end inclusive: three windows, each one from the day after the end of the previous one", async () => {
        fake = fakePagedApi([[{ id: 1 }, { id: 2 }], [{ id: 3 }]])
        config.apiConnectorConfig.apiUrls = [{ name: "W", url: URL_A, pagination, incremental: true, incrementalParams: ymdParams("inclusive") }]
        await pollAPI()
        assert.deepEqual(windowsOf(fake.requests), [[daysAgo(30), daysAgo(21)], [daysAgo(20), daysAgo(11)], [daysAgo(10), daysAgo(1)]])
        // every window from its first page, all its pages
        assert.deepEqual(fake.requests.map(r => r.offset), [0, 2, 4, 0, 2, 4, 0, 2, 4])
        assert.equal((await db.docs(Source)).length, 9)
        assert.deepEqual(await db.docs(Status), []) // the poll is complete
    })

    test("end exclusive: the end of a window is the start of the next one", async () => {
        fake = fakePagedApi([])
        config.apiConnectorConfig.apiUrls = [{ name: "W", url: URL_A, pagination, incremental: true, incrementalParams: ymdParams("exclusive") }]
        await pollAPI()
        assert.deepEqual(windowsOf(fake.requests), [[daysAgo(30), daysAgo(20)], [daysAgo(20), daysAgo(10)], [daysAgo(10), daysAgo(0)]])
    })

    test("an API with no data at all: one request per window, then it stops", async () => {
        fake = fakePagedApi([])
        config.apiConnectorConfig.apiUrls = [{ name: "W", url: URL_A, pagination, incremental: true, incrementalParams: ymdParams("inclusive") }]
        await pollAPI()
        assert.equal(fake.requests.length, 3)
        assert.equal((await db.docs(Source)).length, 0)
    })

    test("a window that fails: the poll is not complete, the next one deletes its records and starts again", async () => {
        fake = fakePagedApi([[{ id: 1 }]])
        config.apiConnectorConfig.apiUrls = [{ name: "W", url: URL_A, pagination, incremental: true, incrementalParams: ymdParams("inclusive") }]
        const get = axios.get
        let calls = 0
        axios.get = async (url, options) => {
            if (++calls == 3) // the first page of the second window
                throw new Error("connection reset")
            return get(url, options)
        }
        await pollAPI()
        assert.equal((await db.docs(Source)).length, 1)
        axios.get = get
        fake.requests.length = 0
        await pollAPI()
        assert.equal(fake.requests[0].params.from, daysAgo(30)) // from startDateValue again
        assert.equal(windowsOf(fake.requests).length, 3)
        assert.equal((await db.docs(Source)).length, 3)
    })

    test("a window shorter than maxWindow: one request, as without maxWindow", async () => {
        fake = fakePagedApi([])
        config.apiConnectorConfig.apiUrls = [{ name: "W", url: URL_A, pagination, incremental: true, incrementalParams: ymdParams("inclusive", { maxWindow: 60 * DAY }) }]
        await pollAPI()
        assert.deepEqual(windowsOf(fake.requests), [[daysAgo(30), daysAgo(1)]])
    })

    test("maxWindow without endDateParam: ignored, one request", async () => {
        fake = fakePagedApi([])
        const { endDateParam, ...noEnd } = ymdParams("inclusive")
        config.apiConnectorConfig.apiUrls = [{ name: "W", url: URL_A, pagination, incremental: true, incrementalParams: noEnd }]
        await pollAPI()
        assert.deepEqual(fake.requests.map(r => r.params.from), [daysAgo(30)])
    })
})

describe("millis, from the last poll (25 days ago), maxWindow 10 days", () => {
    test("end inclusive: windows of 10 days, each one from the millisecond after the end of the previous one", async () => {
        const lastPoll = Date.now() - 25 * DAY
        await records.storeRecords([records.makeItem({ id: 0, datePolled: new Date(lastPoll) }, URL_A)], URL_A)
        fake = fakePagedApi([])
        config.apiConnectorConfig.apiUrls = [{
            name: "W", url: URL_A, pagination, incremental: true, incrementalParams: {
                startDateParam: "from", startDateFormat: "millis", endDateParam: "to", endDateFormat: "millis",
                startDateLogic: "inclusive", endDateLogic: "inclusive", maxWindow: 10 * DAY
            }
        }]
        await pollAPI()
        const windows = windowsOf(fake.requests)
        assert.equal(windows.length, 3)
        assert.equal(windows[0][0], lastPoll)
        for (let k = 0; k < windows.length; k++) {
            assert.ok(windows[k][1] - windows[k][0] < 10 * DAY)
            if (k)
                assert.equal(windows[k][0], windows[k - 1][1] + 1)
        }
        assert.ok(Date.now() - windows[2][1] < 60000) // the last one up to now
    })
})

describe("cursor pagination whose offset is the start date (since)", () => {
    test("each window starts from its date, not from the configured startFrom", async () => {
        const requests = []
        const get = axios.get
        axios.get = async (url, options) => { requests.push({ ...options.params }); return { data: [] } }
        fake = { restore: () => { axios.get = get } }
        config.apiConnectorConfig.apiUrls = [{
            name: "W", url: URL_A, incremental: true,
            pagination: { offsetParam: "from", limitParam: "limit", limit: 100, offsetValue: { from: "response", value: "next", startFrom: "2000-01-01" }, condition: r => r.data?.[0]?.truncated },
            incrementalParams: ymdParams("inclusive")
        }]
        await pollAPI()
        assert.deepEqual(requests.map(r => [r.from, r.to]), [[daysAgo(30), daysAgo(21)], [daysAgo(20), daysAgo(11)], [daysAgo(10), daysAgo(1)]])
    })
})

describe("not paginated", () => {
    const api = () => ({ name: "W", url: URL_A, incremental: true, incrementalParams: ymdParams("inclusive") })

    test("one request per window, the poll recorded as pending until the last one", async () => {
        fake = fakePagedApi([[{ id: 1 }, { id: 2 }]])
        config.apiConnectorConfig.apiUrls = [api()]
        await pollAPI()
        assert.deepEqual(fake.requests.map(r => [r.params.from, r.params.to]), [[daysAgo(30), daysAgo(21)], [daysAgo(20), daysAgo(11)], [daysAgo(10), daysAgo(1)]])
        assert.equal((await db.docs(Source)).length, 6)
        assert.deepEqual(await db.docs(Status), [])
        // already polled today: no request
        fake.requests.length = 0
        await pollAPI()
        assert.deepEqual(fake.requests, [])
    })

    test("interrupted at the second window: its records deleted at the next poll, which starts again", async () => {
        fake = fakePagedApi([[{ id: 1 }, { id: 2 }]])
        config.apiConnectorConfig.apiUrls = [api()]
        const get = axios.get
        let calls = 0
        axios.get = async (url, options) => {
            if (++calls == 2)
                throw new Error("connection reset")
            return get(url, options)
        }
        await pollAPI()
        assert.equal((await db.docs(Source)).length, 2)
        assert.equal((await db.docs(Status)).filter(doc => doc.pending).length, 2)
        axios.get = get
        fake.requests.length = 0
        await pollAPI()
        assert.equal(fake.requests[0].params.from, daysAgo(30))
        assert.equal(fake.requests.length, 3)
        assert.equal((await db.docs(Source)).length, 6)
        assert.deepEqual(await db.docs(Status), [])
    })
})
