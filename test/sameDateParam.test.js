// Incremental APIs with date prefixes (incrementalParams.startDatePrefix / endDatePrefix) and the same param for start
// and end, as a FHIR server: ?_lastUpdated=ge<start>&_lastUpdated=lt<end>. On real MongoDB + PostgreSQL (helpers/db.js).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")
const { loadApiConnector } = require("./helpers/apiConnector")

const URL_F = "https://fhir.example/baseDstu3/Appointment"
const DAY = 24 * 3600 * 1000
let pollAPI, prepareIncremental, requestParams, Source, realGet

before(async () => {
    await db.setup(__filename)
    Source = load("utils/collections.js").collectionModel("api")
    await load("utils/pollStatus.js").ensurePgStatusTable()
    pollAPI = await loadApiConnector()
    ;({ prepareIncremental, requestParams } = load("inputConnectors/apiConnector.js"))
    realGet = axios.get
})
after(db.teardown)
beforeEach(async () => {
    await db.clean()
    await db.query("TRUNCATE status RESTART IDENTITY")
})
afterEach(() => { axios.get = realGet })

const fhirParams = (extra = {}) => ({
    startDateParam: "_lastUpdated", endDateParam: "_lastUpdated", startDatePrefix: "ge", endDatePrefix: "lt",
    startDateFormat: "RFC 3339", endDateFormat: "RFC 3339", startDateLogic: "inclusive", endDateLogic: "exclusive",
    startDateValue: "2026-01-01T00:00:00Z", maxWindow: DAY, alignToDays: true, ...extra
})
const fhirApi = (extra = {}, paramsExtra = {}) => ({ name: "FHIR", url: URL_F, queryParams: { _count: 1000 }, incremental: true, incrementalParams: fhirParams(paramsExtra), ...extra })

describe("date prefixes and the same param for start and end", () => {
    test("the windows: both dates, prefixed, under the same param", () => {
        const prepared = prepareIncremental(undefined, fhirApi(), new Date("2026-01-03T10:00:00Z"), null)
        assert.deepEqual(prepared.windows, [
            { _lastUpdated: ["ge2026-01-01T00:00:00.000Z", "lt2026-01-02T00:00:00.000Z"] },
            { _lastUpdated: ["ge2026-01-02T00:00:00.000Z", "lt2026-01-03T00:00:00.000Z"] }
        ])
    })

    test("startDateValue sent as configured (without alignToDays), with its prefix", () => {
        const prepared = prepareIncremental(undefined, fhirApi({}, { alignToDays: undefined, maxWindow: undefined }), new Date("2026-01-03T10:00:00Z"), null)
        assert.deepEqual(prepared.windows, [{ _lastUpdated: ["ge2026-01-01T00:00:00Z", "lt2026-01-03T10:00:00.000Z"] }])
    })

    test("two params: each with its prefix; no prefix: as before", () => {
        const two = prepareIncremental(undefined, fhirApi({}, { startDateParam: "from", endDateParam: "to", startDatePrefix: "gt" }), new Date("2026-01-02T10:00:00Z"), null)
        assert.deepEqual(two.windows, [{ from: "gt2026-01-01T00:00:00.000Z", to: "lt2026-01-02T00:00:00.000Z" }])
        const none = prepareIncremental(undefined, fhirApi({}, { startDateParam: "from", endDateParam: "to", startDatePrefix: undefined, endDatePrefix: "" }), new Date("2026-01-02T10:00:00Z"), null)
        assert.deepEqual(none.windows, [{ from: "2026-01-01T00:00:00.000Z", to: "2026-01-02T00:00:00.000Z" }])
    })

    test("no start date (no startDateValue): only the end, prefixed", () => {
        const prepared = prepareIncremental(undefined, fhirApi({}, { startDateValue: undefined }), new Date("2026-01-03T10:00:00Z"), null)
        assert.deepEqual(prepared.windows, [{ _lastUpdated: "lt2026-01-03T00:00:00.000Z" }])
    })

    test("the url: the param repeated (p=a&p=b), not p[]=; other APIs as before", () => {
        const params = { _count: 1000, _lastUpdated: ["ge2026-01-01T00:00:00.000Z", "lt2026-01-02T00:00:00.000Z"] }
        assert.equal(axios.getUri({ url: URL_F, ...requestParams(fhirApi(), params) }),
            URL_F + "?_count=1000&_lastUpdated=ge2026-01-01T00:00:00.000Z&_lastUpdated=lt2026-01-02T00:00:00.000Z")
        const other = { name: "O", url: URL_F, incremental: true, incrementalParams: { startDateParam: "from", endDateParam: "to" } }
        assert.deepEqual(requestParams(other, { a: [1, 2] }), { params: { a: [1, 2] } })
    })

    for (const paginated of [false, true])
        test(`a poll${paginated ? " (paginated)" : ""}: the requests as the server expects them, the records with the dates sent`, async () => {
            const urls = []
            axios.get = async (url, options) => {
                urls.push(axios.getUri({ url, ...options }))
                const offset = Number(options?.params?.offset ?? 0)
                return { data: offset ? [] : [{ resourceType: "Bundle", n: urls.length }] }
            }
            const pagination = { offsetParam: "offset", limitParam: "limit", limit: 1, condition: response => response.data.length > 0 }
            config.apiConnectorConfig.apiUrls = [fhirApi(paginated ? { pagination } : {}, { startDateValue: new Date(Date.now() - 2 * DAY).toISOString().split("T")[0] + "T00:00:00Z" })]
            await pollAPI()
            const day = k => new Date(Date.now() - k * DAY).toISOString().split("T")[0] + "T00:00:00.000Z"
            const dates = urls.map(u => new URL(u).searchParams.getAll("_lastUpdated"))
            assert.ok(urls.every(u => !u.includes("[]")), urls.join("\n"))
            assert.deepEqual([...new Set(dates.map(d => d.join(" ")))], [`ge${day(2)} lt${day(1)}`, `ge${day(1)} lt${day(0)}`])
            const stored = (await Source.find({}).sort({ _id: 1 }).lean()).filter(r => r.resourceType)
            assert.deepEqual(stored.map(r => r._lastUpdated), [[`ge${day(2)}`, `lt${day(1)}`], [`ge${day(1)}`, `lt${day(0)}`]])
        })
})
