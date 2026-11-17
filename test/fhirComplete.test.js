// fhir: true (or completeAnswer): an answer without every record asked (a FHIR Bundle with fewer entries than its total:
// a server without paging) is an error and nothing of it is stored. On real MongoDB + PostgreSQL (helpers/db.js).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")
const { loadApiConnector } = require("./helpers/apiConnector")

const URL_F = "https://fhir.example/baseDstu3/Appointment"
const DAY = 24 * 3600 * 1000
const day = k => new Date(Date.now() - k * DAY).toISOString().split("T")[0] + "T00:00:00.000Z"
let pollAPI, fhirComplete, Source, state, realGet

before(async () => {
    await db.setup(__filename)
    Source = load("utils/collections.js").collectionModel("api")
    state = load("utils/incrementalState.js")
    await load("utils/pollStatus.js").ensurePgStatusTable()
    pollAPI = await loadApiConnector()
    ;({ fhirComplete } = load("inputConnectors/apiConnector.js"))
    realGet = axios.get
})
after(db.teardown)
beforeEach(async () => {
    await db.clean()
    await db.query("TRUNCATE status RESTART IDENTITY")
})
afterEach(() => { axios.get = realGet })

const bundle = (total, entries) => ({ resourceType: "Bundle", type: "searchset", total, entry: Array.from({ length: entries }, (_, i) => ({ resource: { id: String(i) } })) })
const fhirApi = (extra = {}) => ({
    name: "FHIR", url: URL_F, fhir: true, queryParams: { _count: 3 }, incremental: true,
    incrementalParams: {
        startDateParam: "_lastUpdated", endDateParam: "_lastUpdated", startDatePrefix: "ge", endDatePrefix: "lt",
        startDateFormat: "RFC 3339", endDateFormat: "RFC 3339", startDateLogic: "inclusive", endDateLogic: "exclusive",
        startDateValue: day(2).replace(".000", ""), maxWindow: DAY, alignToDays: true
    },
    ...extra
})
// axios.get answering by the start date of the window: answers[start] (default: complete); the requests recorded
function fakeServer(answers = {}) {
    const asked = []
    axios.get = async (url, options) => {
        const start = [].concat(options?.params?._lastUpdated ?? [])[0]
        asked.push(start)
        return { data: structuredClone(answers[start] ?? bundle(2, 2)) }
    }
    return asked
}
const bundles = async () => (await Source.find({ resourceType: "Bundle" }).sort({ _id: 1 }).lean()).map(r => [r.total, r.entry.length])

describe("complete answers", () => {
    test("fhirComplete: entries >= total; no total: nothing to check", () => {
        assert.equal(fhirComplete({ data: bundle(3, 3) }), true)
        assert.equal(fhirComplete({ data: bundle(0, 0) }), true)
        assert.equal(fhirComplete({ data: { resourceType: "Bundle", total: 0 } }), true)
        assert.equal(fhirComplete({ data: bundle(6, 3) }), false)
        assert.equal(fhirComplete({ data: { resourceType: "Bundle", total: 2 } }), false)
        assert.equal(fhirComplete({ data: { resourceType: "Bundle", entry: [] } }), true)
        assert.equal(fhirComplete({ data: [1, 2] }), true)
    })

    test("incremental: the incomplete window and the next ones not stored; the next poll asks again from it", async () => {
        let asked = fakeServer({ ["ge" + day(1)]: bundle(6, 3) })
        config.apiConnectorConfig.apiUrls = [fhirApi()]
        await pollAPI()
        assert.deepEqual(asked, ["ge" + day(2), "ge" + day(1)])
        assert.deepEqual(await bundles(), [[2, 2]]) // only the first window
        const saved = await state.get(URL_F)
        assert.equal(new Date(saved.lastEnd).toISOString(), day(1))
        assert.equal(saved.complete, false)
        // the server answers it whole now: the poll resumes from that window
        asked = fakeServer({ ["ge" + day(1)]: bundle(6, 6) })
        await pollAPI()
        assert.deepEqual(asked, ["ge" + day(1)])
        assert.deepEqual(await bundles(), [[2, 2], [6, 6]])
    })

    test("not incremental: an incomplete answer leaves the records stored as they were", async () => {
        const api = { name: "FHIR all", url: URL_F, fhir: true, upsertRecords: true }
        config.apiConnectorConfig.apiUrls = [api]
        axios.get = async () => ({ data: bundle(2, 2) })
        await pollAPI()
        axios.get = async () => ({ data: bundle(9, 3) })
        await pollAPI()
        assert.deepEqual(await bundles(), [[2, 2]])
    })

    test("completeAnswer: any check; without fhir nor completeAnswer, nothing checked", async () => {
        config.apiConnectorConfig.apiUrls = [{ name: "Custom", url: URL_F, upsertRecords: true, completeAnswer: response => response.data.ok === true }]
        axios.get = async () => ({ data: { ok: false } })
        await pollAPI()
        assert.equal((await Source.find({}).lean()).length, 0)
        config.apiConnectorConfig.apiUrls = [{ name: "Plain", url: URL_F, upsertRecords: true }]
        axios.get = async () => ({ data: bundle(9, 3) })
        await pollAPI()
        assert.deepEqual(await bundles(), [[9, 3]])
    })

    test("fhir with pagination: the pages are not checked against the total", async () => {
        const pagination = { offsetParam: "_offset", limitParam: "_count", limit: 3, condition: response => (response.data.entry?.length || 0) > 0 }
        config.apiConnectorConfig.apiUrls = [{ name: "Paged", url: URL_F, fhir: true, upsertRecords: true, pagination }]
        axios.get = async (url, options) => ({ data: options.params._offset ? bundle(3, 0) : bundle(3, 3) })
        await pollAPI()
        assert.ok((await bundles()).some(([total, entries]) => total == 3 && entries == 3))
    })
})
