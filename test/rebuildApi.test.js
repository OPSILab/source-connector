// POST/GET /rebuild through the real router and auth middleware (service.js is stubbed: it starts the
// MinIO/Orion connectors at require time). No database needed: the rebuild itself is replaced by a spy.
const { load, stub, config, resetConfig } = require("./helpers/env")
const { publicKey, otherPrivateKey, makeToken } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

stub("api/services/service.js", {})
// loaded by index.js in the service; here a spy (requiring the real one would start polling)
const polled = []
stub("inputConnectors/apiConnector.js", { pollAPI: async apis => { polled.push(apis) } })
const router = load("api/routes/router.js")
const rebuild = load("utils/rebuild.js")
const locks = load("utils/jobLocks.js")

let server, baseUrl, runCalls
const realRunRebuild = rebuild.runRebuild

before(async () => {
    const app = express()
    app.use(express.json())
    app.use("/api", router)
    await new Promise(resolve => { server = app.listen(0, "127.0.0.1", resolve) })
    baseUrl = `http://127.0.0.1:${server.address().port}/api`
})

after(() => {
    rebuild.runRebuild = realRunRebuild
    server?.close()
})

beforeEach(() => {
    resetConfig()
    locks.polling = false
    locks.rebuilding = false
    runCalls = []
    rebuild.runRebuild = async params => { runCalls.push(params) }
    Object.assign(config.authConfig, { disableAuth: true, clientId: "query-engine", publicKey, userInfoEndpoint: "", introspect: false })
})

const post = (path, { body, token, headers = {} } = {}) => fetch(baseUrl + path, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: "Bearer " + token } : {}), ...headers },
    body: body && JSON.stringify(body)
})

describe("POST /rebuild", () => {
    test("400 without a mode: never \"all\" by default", async () => {
        const res = await post("/rebuild")
        assert.equal(res.status, 400)
        assert.match(await res.text(), /mode is required/)
        assert.equal(runCalls.length, 0)
    })

    test("202 and starts the rebuild in background", async () => {
        const res = await post("/rebuild?mode=all")
        assert.equal(res.status, 202)
        assert.deepEqual(await res.json(), { started: true, mode: "all", keepLegacy: false })
        assert.deepEqual(runCalls, [{ mode: "all", connector: undefined, origin: undefined, keepLegacy: false }])
    })

    test("parameters from the query string or from the body", async () => {
        await post("/rebuild?mode=entries&connector=api&origin=" + encodeURIComponent("https://api/a?x=1") + "&keepLegacy=true")
        await post("/rebuild", { body: { mode: "postgres", connector: "orion", origin: "https://api/b", keepLegacy: true } })
        assert.deepEqual(runCalls, [
            { mode: "entries", connector: "api", origin: "https://api/a?x=1", keepLegacy: true },
            { mode: "postgres", connector: "orion", origin: "https://api/b", keepLegacy: true }
        ])
    })

    test("400 on an unknown connector, or an origin without connector", async () => {
        assert.equal((await post("/rebuild?mode=entries&connector=nope")).status, 400)
        assert.equal((await post("/rebuild?mode=entries&origin=https://api/a")).status, 400)
        assert.equal(runCalls.length, 0)
    })

    test("400 on an unknown mode", async () => {
        const res = await post("/rebuild?mode=everything")
        assert.equal(res.status, 400)
        assert.match(await res.text(), /Unknown mode/)
        assert.equal(runCalls.length, 0)
    })

    test("409 while a rebuild or a poll is running", async () => {
        locks.rebuilding = true
        assert.equal((await post("/rebuild?mode=entries")).status, 409)
        locks.rebuilding = false
        locks.polling = true
        const res = await post("/rebuild?mode=entries")
        assert.equal(res.status, 409)
        assert.match(await res.text(), /poll/)
        assert.equal(runCalls.length, 0)
    })
})

describe("GET /rebuild", () => {
    test("returns the status of the last rebuild", async () => {
        rebuild.runRebuild = realRunRebuild
        config.queryOptions.SQLQuery = false
        await realRunRebuild({ mode: "postgres" })
        const res = await fetch(baseUrl + "/rebuild")
        assert.equal(res.status, 200)
        const status = await res.json()
        assert.equal(status.running, false)
        assert.equal(status.mode, "postgres")
        assert.match(status.result.postgres.api, /skipped/)
    })
})

describe("auth on /rebuild", () => {
    beforeEach(() => { config.authConfig.disableAuth = false })

    test("401 without a token", async () => {
        assert.equal((await post("/rebuild?mode=entries")).status, 401)
        assert.equal((await fetch(baseUrl + "/rebuild")).status, 401)
        assert.equal(runCalls.length, 0)
    })

    test("202 with a valid token for the configured client", async () => {
        const res = await post("/rebuild?mode=entries", { token: makeToken({ azp: "query-engine", email: "a@b.it" }) })
        assert.equal(res.status, 202)
    })

    test("401 invalid_token (RFC 6750) with an expired token, a token of another client, malformed or signed with another key", async () => {
        for (const token of [makeToken({ azp: "query-engine" }, { expiresIn: -60 }), makeToken({ azp: "someone-else" }), "not.a.jwt",
            makeToken({ azp: "query-engine" }, { key: otherPrivateKey })]) {
            const res = await post("/rebuild?mode=entries", { token })
            assert.equal(res.status, 401)
            assert.match(res.headers.get("www-authenticate"), /^Bearer error="invalid_token"/)
        }
        assert.equal((await post("/rebuild?mode=entries")).headers.get("www-authenticate"), "Bearer")
        assert.equal(runCalls.length, 0)
    })
})

describe("POST /incremental/reset", () => {
    const reset = load("utils/resetIncremental.js")
    const realReset = reset.resetIncremental
    let resetCalls
    beforeEach(() => {
        resetCalls = []
        config.managementSecret = "s3cret"
        reset.resetIncremental = async params => { resetCalls.push(params); return { dryRun: !!params.dryRun, apis: [] } }
        config.apiConnectorConfig.apiUrls = [
            { name: "Incr A", url: "https://a.example", incremental: true },
            { name: "Incr B", url: "https://b.example", incremental: true },
            { name: "Plain", url: "https://c.example" }
        ]
    })
    after(() => { reset.resetIncremental = realReset })
    const SECRET = { "X-Management-Secret": "s3cret" }
    const resetPost = (path, options = {}) => post(path, { ...options, headers: SECRET })

    test("403 without the management password, with a wrong one, or when none is configured; the dry run does not need it", async () => {
        for (const headers of [{}, { "X-Management-Secret": "wrong" }]) {
            const res = await post("/incremental/reset", { headers })
            assert.equal(res.status, 403)
            assert.match(await res.text(), /management password/)
        }
        config.managementSecret = ""
        const res = await resetPost("/incremental/reset")
        assert.deepEqual([res.status, await res.text()], [403, "Disabled: managementSecret is not set in the configuration of the Source-Connector"])
        assert.equal((await post("/incremental/reset?dryRun=true")).status, 200)
        assert.deepEqual(resetCalls.map(call => !!call.dryRun), [true])
    })

    test("who asked is passed on (logged): the e-mail of the token", async () => {
        config.authConfig.disableAuth = false
        const res = await post("/incremental/reset", { token: makeToken({ azp: "query-engine", email: "anna@demetrix.it" }), headers: SECRET })
        assert.equal(res.status, 202)
        assert.equal(resetCalls[0].by, "anna@demetrix.it")
    })

    test("202: every incremental API, or those named (query string or body), with apiConnector's pollAPI", async () => {
        let res = await resetPost("/incremental/reset")
        assert.deepEqual([res.status, await res.json()], [202, { started: true, apis: ["Incr A", "Incr B"] }])
        res = await resetPost("/incremental/reset?name=Incr%20B")
        assert.deepEqual((await res.json()).apis, ["Incr B"])
        res = await resetPost("/incremental/reset", { body: { names: ["Incr A"] } })
        assert.deepEqual((await res.json()).apis, ["Incr A"])
        assert.deepEqual(resetCalls.map(call => call.names), [[], ["Incr B"], ["Incr A"]])
        assert.ok(resetCalls.every(call => typeof call.pollAPI == "function"))
    })

    test("400 for a name that is not an incremental API; 409 while busy or without apiConnector", async () => {
        assert.equal((await resetPost("/incremental/reset?name=Plain")).status, 400)
        locks.polling = true
        assert.equal((await resetPost("/incremental/reset")).status, 409)
        locks.polling = false
        config.sourceConnectors.apiConnector = false
        assert.equal((await resetPost("/incremental/reset")).status, 409)
        assert.equal(resetCalls.length, 0)
    })

    test("dryRun: the counts at once (200)", async () => {
        const res = await post("/incremental/reset?dryRun=true")
        assert.deepEqual([res.status, await res.json()], [200, { dryRun: true, apis: [] }])
        assert.deepEqual(resetCalls, [{ names: [], dryRun: true }])
    })
})
