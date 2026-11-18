// pagination.offsetIncrement: "limit" (default: the offset grows by the records of a page) or "index" (a page
// number: page=1, 2, 3...). On real MongoDB + PostgreSQL (helpers/db.js).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")
const { loadApiConnector } = require("./helpers/apiConnector")

const URL_D = "https://directory.example/api/data/v1/directory/businesses"
let pollAPI, Source, realGet

before(async () => {
    await db.setup(__filename)
    Source = load("utils/collections.js").collectionModel("api")
    await load("utils/pollStatus.js").ensurePgStatusTable()
    pollAPI = await loadApiConnector()
    realGet = axios.get
})
after(db.teardown)
beforeEach(async () => {
    await db.clean()
    await db.query("TRUNCATE status RESTART IDENTITY")
})
afterEach(() => { axios.get = realGet })

// 7 businesses, answered as the directory does: { data, meta: { page, per_page, total }, links: { next } }
const BUSINESSES = Array.from({ length: 7 }, (_, i) => ({ id: "b" + i }))
function fakeDirectory() {
    const asked = []
    axios.get = async (url, options) => {
        const { page, per_page } = options.params
        asked.push([page, per_page])
        const data = BUSINESSES.slice((page - 1) * per_page, page * per_page)
        const last = page * per_page >= BUSINESSES.length
        return { data: { data, meta: { generated_at: new Date().toISOString(), page, per_page, total: BUSINESSES.length }, links: last ? {} : { next: `${url}?page=${page + 1}` } } }
    }
    return asked
}
const directory = (pagination = {}) => ({
    name: "Directory", url: URL_D, queryParams: { format: "json" }, upsertRecords: true,
    pagination: { offsetParam: "page", limitParam: "per_page", limit: 3, offsetValue: { startFrom: 1 }, condition: response => !!response.data.links?.next, ...pagination }
})
const stored = async () => (await Source.find({}).sort({ _id: 1 }).lean()).flatMap(r => r.data.map(b => b.id))

describe("offsetIncrement", () => {
    test('"index": page 1, 2, 3 with per_page 3, every business once', async () => {
        const asked = fakeDirectory()
        config.apiConnectorConfig.apiUrls = [directory({ offsetIncrement: "index" })]
        await pollAPI()
        assert.deepEqual(asked, [[1, 3], [2, 3], [3, 3]])
        assert.deepEqual(await stored(), BUSINESSES.map(b => b.id))
        // the next poll replaces them (upsertRecords): still once each
        await pollAPI()
        assert.deepEqual(await stored(), BUSINESSES.map(b => b.id))
    })

    test('default ("limit"): the offset grows by the limit, as before (here a page number: page 4 is past the end)', async () => {
        const asked = fakeDirectory()
        config.apiConnectorConfig.apiUrls = [directory()]
        await pollAPI()
        assert.deepEqual(asked, [[1, 3], [4, 3]])
    })

    test("unknown value: by the limit", async () => {
        const asked = fakeDirectory()
        config.apiConnectorConfig.apiUrls = [directory({ offsetIncrement: "pages" })]
        await pollAPI()
        assert.deepEqual(asked, [[1, 3], [4, 3]])
    })
})

describe("pagination.condition on an answer that is not a list", () => {
    test("on the answer as it is (AgroTour: response.data.meta?.has_more): every page read", async () => {
        const asked = []
        axios.get = async (url, options) => {
            const offset = options.params.offset
            asked.push(offset)
            const data = BUSINESSES.slice(offset, offset + 3)
            return { data: { data, meta: { has_more: offset + 3 < BUSINESSES.length } } }
        }
        config.apiConnectorConfig.apiUrls = [{ name: "AgroTour-like", url: URL_D, upsertRecords: true,
            pagination: { offsetParam: "offset", limitParam: "limit", limit: 3, condition: response => response.data.meta?.has_more } }]
        await pollAPI()
        assert.deepEqual(asked, [0, 3, 6])
        assert.deepEqual(await stored(), BUSINESSES.map(b => b.id))
    })

    test("as a list of it, as before (Regrowth: response.data[0]?.truncated, a cursor from the answer)", async () => {
        const asked = []
        axios.get = async (url, options) => {
            const since = options.params.since
            asked.push(since)
            const k = ["2025-01-01T00:00:00Z", "c1", "c2"].indexOf(since)
            return { data: { data: [BUSINESSES[k]], truncated: k < 2, next_since: ["c1", "c2", null][k] } }
        }
        config.apiConnectorConfig.apiUrls = [{ name: "Regrowth-like", url: URL_D, upsertRecords: true,
            pagination: { offsetParam: "since", limitParam: "limit", limit: 1, offsetValue: { from: "response", value: "next_since", startFrom: "2025-01-01T00:00:00Z", type: "RFC 3339" }, condition: response => response?.data?.[0]?.truncated } }]
        await pollAPI()
        assert.deepEqual(asked, ["2025-01-01T00:00:00Z", "c1", "c2"])
    })

    test("a condition failing on both shapes: the poll fails, nothing replaced", async () => {
        axios.get = async () => ({ data: { data: [{ id: "x" }] } })
        config.apiConnectorConfig.apiUrls = [{ name: "Broken", url: URL_D, upsertRecords: true,
            pagination: { offsetParam: "offset", limitParam: "limit", limit: 3, condition: response => response.data.nothing.here } }]
        await pollAPI()
        assert.deepEqual(await stored(), [])
    })
})
