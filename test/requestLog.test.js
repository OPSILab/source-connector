// The axios requests, failed and successful, saved in the statuslog collection (utils/requestLog.js), on real MongoDB
// (see helpers/db.js), against a local HTTP server.
const { load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const http = require("http")
const axios = require("axios")

let server, base, requestLog, StatusLog

before(async () => {
    await db.setup(__filename)
    StatusLog = load("api/models/StatusLog.js")
    requestLog = load("utils/requestLog.js")
    server = http.createServer((req, res) => {
        const url = new URL(req.url, "http://localhost")
        res.setHeader("Content-Type", "application/json")
        if (url.pathname == "/ok")
            return res.end(JSON.stringify({ ok: true }))
        if (url.pathname == "/fhir")
            return res.end(JSON.stringify({ resourceType: "Bundle", entry: [{ resource: { participant: [{ actor: { display: "Mario Rossi" } }] } }] }))
        if (url.pathname == "/big") {
            res.statusCode = 502
            return res.end(JSON.stringify({ message: "x".repeat(100) }))
        }
        res.statusCode = 500
        res.end(JSON.stringify({ message: "boom", path: url.pathname }))
    })
    await new Promise(resolve => server.listen(0, "127.0.0.1", resolve))
    base = `http://127.0.0.1:${server.address().port}`
    requestLog.install()
})
after(async () => {
    requestLog.uninstall()
    server?.close()
    await db.teardown()
})
beforeEach(async () => {
    resetConfig()
    await StatusLog.collection.deleteMany({})
})

const docs = async () => {
    await requestLog.settled()
    return (await StatusLog.collection.find({}).toArray()).map(({ _id, ...doc }) => doc)
}

describe("statuslog", () => {
    test("an error answer: url, params, headers and body as they are, status and response data; the caller gets the same error", async () => {
        config.apiConnectorConfig.apiUrls = [{ name: "Records", url: base + "/records?fixed=1" }]
        const error = await axios.post(base + "/records", { id: 7 }, { params: { from: "2026-10-01", to: "2026-10-31" }, headers: { Authorization: "Bearer secret-token", "X-Test": "yes" } })
            .catch(error => error)
        assert.equal(error.response.status, 500) // unchanged for the caller
        const [doc] = await docs()
        assert.equal(doc.api, "Records")
        assert.equal(doc.method, "POST")
        assert.equal(doc.url, base + "/records")
        assert.equal(doc.fullUrl, base + "/records?from=2026-10-01&to=2026-10-31")
        assert.deepEqual(doc.params, { from: "2026-10-01", to: "2026-10-31" })
        assert.equal(doc.headers.Authorization, "Bearer secret-token") // in clear, as decided
        assert.equal(doc.headers["X-Test"], "yes")
        assert.equal(doc.body, JSON.stringify({ id: 7 }))
        assert.equal(doc.status, 500)
        assert.equal(doc.data, JSON.stringify({ message: "boom", path: "/records" }))
        assert.ok(Date.now() - new Date(doc.createdAt).getTime() < 60000)
        assert.equal(doc.host, new URL(base).host)
    })

    test("no answer (connection refused): the error and its code, no status", async () => {
        const closed = http.createServer()
        await new Promise(resolve => closed.listen(0, "127.0.0.1", resolve))
        const url = `http://127.0.0.1:${closed.address().port}/down`
        await new Promise(resolve => closed.close(resolve))
        await axios.get(url).catch(() => { })
        const [doc] = await docs()
        assert.equal(doc.url, url)
        assert.equal(doc.code, "ECONNREFUSED")
        assert.equal(doc.status, undefined)
        assert.ok(doc.error)
    })

    test("a successful request: saved with outcome success, status, data and duration; it expires after successRetentionDays", async () => {
        await axios.get(base + "/ok", { params: { a: 1 } })
        const [doc] = await docs()
        assert.equal(doc.outcome, "success")
        assert.equal(doc.method, "GET")
        assert.equal(doc.fullUrl, base + "/ok?a=1")
        assert.equal(doc.status, 200)
        assert.equal(doc.data, JSON.stringify({ ok: true }))
        assert.equal(typeof doc.durationMs, "number")
        assert.ok(doc.durationMs >= 0)
        assert.equal(doc.error, undefined)
        const days = (new Date(doc.expiresAt) - new Date(doc.createdAt)) / (24 * 60 * 60 * 1000)
        assert.equal(days, 30)
    })

    test("successRetentionDays: the expiry of the successes; 0: kept forever", async () => {
        config.statusLog.successRetentionDays = 2
        await axios.get(base + "/ok")
        const [doc] = await docs()
        assert.equal((new Date(doc.expiresAt) - new Date(doc.createdAt)) / (24 * 60 * 60 * 1000), 2)
        await StatusLog.collection.deleteMany({})
        config.statusLog.successRetentionDays = 0
        await axios.get(base + "/ok")
        const [forever] = await docs()
        assert.equal(forever.outcome, "success")
        assert.equal(forever.expiresAt, undefined)
    })

    test("statusLog.successes false: only the errors saved", async () => {
        config.statusLog.successes = false
        await axios.get(base + "/ok")
        await axios.get(base + "/fail").catch(() => { })
        assert.deepEqual((await docs()).map(doc => [doc.outcome, doc.status]), [["error", 500]])
    })

    test("the errors: outcome error, never expire", async () => {
        await axios.get(base + "/fail").catch(() => { })
        const [doc] = await docs()
        assert.equal(doc.outcome, "error")
        assert.equal(doc.expiresAt, undefined)
        assert.equal(typeof doc.durationMs, "number")
    })

    test("response data longer than maxDataSize: cut, with its length", async () => {
        config.statusLog.maxDataSize = 20
        await axios.get(base + "/big").catch(() => { })
        const [doc] = await docs()
        assert.equal(doc.data.length, 20)
        assert.equal(doc.dataTruncated, true)
        assert.equal(doc.dataLength, JSON.stringify({ message: "x".repeat(100) }).length)
    })

    test("statusLog.enabled false: nothing saved", async () => {
        config.statusLog.enabled = false
        await axios.get(base + "/fail").catch(() => { })
        assert.deepEqual(await docs(), [])
    })

    test("a request of a batch API (url with {batch}) is attributed to it", async () => {
        config.apiConnectorConfig.apiUrls = [{ name: "Batch", url: base + "/item/{batch}", batch: { from: base + "/ids", param: "id" } }]
        await axios.get(base + "/item/42").catch(() => { })
        await axios.get(base + "/ids").catch(() => { })
        assert.deepEqual((await docs()).map(doc => doc.api), ["Batch", "Batch"])
    })

    test("an API with hideFields: its answer saved with the pseudonyms (the caller gets it as it came); no secret: not saved", async () => {
        const { pseudonym } = load("utils/hideFields.js")
        config.apiConnectorConfig.apiUrls = [{ name: "Appointments", url: base + "/fhir", hideFields: ["entry.resource.participant.actor.display"] }]
        config.hideFieldsSecret = "key"
        const response = await axios.get(base + "/fhir")
        assert.equal(response.data.entry[0].resource.participant[0].actor.display, "Mario Rossi")
        let [doc] = await docs()
        assert.ok(!doc.data.includes("Mario Rossi"))
        assert.ok(doc.data.includes(pseudonym("Mario Rossi", "key")))
        await StatusLog.collection.deleteMany({})
        config.hideFieldsSecret = ""
        await axios.get(base + "/fhir")
        ;[doc] = await docs()
        assert.match(doc.data, /^\[not saved: Appointments: hideFields needs hideFieldsSecret/)
    })
})
