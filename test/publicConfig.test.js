// GET /api/config (utils/publicConfig.js): what anyone may see of the configuration, without anything secret.
const { load } = require("./helpers/env")
const { test, describe } = require("node:test")
const assert = require("node:assert/strict")
const { publicConfig, safeUrl } = load("utils/publicConfig.js")

const DAY = 24 * 3600 * 1000
const cfg = {
    sourceConnectors: { minioConnector: false, orionConnector: true, apiConnector: true },
    apiConnectorConfig: {
        pollInterval: DAY,
        apiUrls: [
            {
                name: "KPIs", url: "https://kpi.example/api/v1/kpis?lang=it&api_key=SECRET1", incremental: true,
                incrementalParams: { startDateParam: "date_from", endDateParam: "date_to", startDateValue: "2026-10-01", startDateFormat: "YYYY-MM-DD", maxWindow: DAY, pollDelay: 1 },
                headers: { "X-API-Key": { type: "bearerToken", authProfile: "hardcoded", value: "SECRET2" } }
            },
            {
                name: "Stations", url: "https://user:SECRET3@st.example/stations/{batch}/readings",
                batch: { from: "https://st.example/stations?token=SECRET4", param: "code" },
                headers: { Authorization: { type: "bearerToken", credentials: { client_id: "c", client_secret: "SECRET5" }, authUrl: { value: "https://auth.example/token" } } },
                upsertRecords: true, body: { password: "SECRET6" }
            },
            { name: "Paged", url: "https://paged.example/items", method: "post", pagination: { offsetParam: "offset", limitParam: "limit", limit: 10 }, incremental: true, incrementalParams: { startDateParam: "since", maxWindow: DAY } }
        ]
    },
    authConfig: { secret: "SECRET7" }, managementSecret: "SECRET8"
}

describe("publicConfig", () => {
    test("the connectors, the poll interval and the APIs, nothing else", () => {
        assert.deepEqual(publicConfig(cfg), {
            sources: { minio: false, orion: true, api: true },
            pollInterval: DAY,
            apis: [
                { name: "KPIs", url: "https://kpi.example/api/v1/kpis?lang=it&api_key=masked", method: "GET", paginated: false, incremental: true, startDateValue: "2026-10-01", maxWindow: DAY },
                { name: "Stations", url: "https://st.example/stations/{batch}/readings", method: "GET", paginated: false, batch: { from: "https://st.example/stations?token=masked" }, incremental: false },
                { name: "Paged", url: "https://paged.example/items", method: "POST", paginated: true, incremental: true, startDateValue: null, maxWindow: DAY, maxWindowIgnored: true }
            ]
        })
        assert.ok(!JSON.stringify(publicConfig(cfg)).includes("SECRET"))
    })

    test("no APIs, no connectors: empty, no error", () => {
        assert.deepEqual(publicConfig({}), { sources: { minio: false, orion: false, api: false }, pollInterval: null, apis: [] })
    })

    test("safeUrl: user and password dropped, credential-like params hidden, a broken url not shown", () => {
        assert.equal(safeUrl("https://a:b@x.example/p?Token=1&page=2&AccessKey=3"), "https://x.example/p?Token=masked&page=2&AccessKey=masked")
        assert.equal(safeUrl("not a url"), "<not a valid url>")
        assert.equal(safeUrl(undefined), undefined)
    })
})
