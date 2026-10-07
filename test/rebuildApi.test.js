// POST/GET /rebuild through the real router and auth middleware (service.js is stubbed: it starts the
// MinIO/Orion connectors at require time). No database needed: the rebuild itself is replaced by a spy.
const { load, stub, config, resetConfig } = require("./helpers/env")
const { publicKey, otherPrivateKey, makeToken } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

stub("api/services/service.js", {})
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

const post = (path, { body, token } = {}) => fetch(baseUrl + path, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: "Bearer " + token } : {}) },
    body: body && JSON.stringify(body)
})

describe("POST /rebuild", () => {
    test("202 and starts the rebuild in background (defaults: mode all)", async () => {
        const res = await post("/rebuild")
        assert.equal(res.status, 202)
        assert.deepEqual(await res.json(), { started: true, mode: "all", keepLegacy: false })
        assert.deepEqual(runCalls, [{ mode: "all", origin: undefined, keepLegacy: false }])
    })

    test("parameters from the query string or from the body", async () => {
        await post("/rebuild?mode=entries&origin=" + encodeURIComponent("https://api/a?x=1") + "&keepLegacy=true")
        await post("/rebuild", { body: { mode: "postgres", origin: "https://api/b", keepLegacy: true } })
        assert.deepEqual(runCalls, [
            { mode: "entries", origin: "https://api/a?x=1", keepLegacy: true },
            { mode: "postgres", origin: "https://api/b", keepLegacy: true }
        ])
    })

    test("400 on an unknown mode", async () => {
        const res = await post("/rebuild?mode=everything")
        assert.equal(res.status, 400)
        assert.match(await res.text(), /Unknown mode/)
        assert.equal(runCalls.length, 0)
    })

    test("409 while a rebuild or a poll is running", async () => {
        locks.rebuilding = true
        assert.equal((await post("/rebuild")).status, 409)
        locks.rebuilding = false
        locks.polling = true
        const res = await post("/rebuild")
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
        assert.match(status.result.postgres, /skipped/)
    })
})

describe("auth on /rebuild", () => {
    beforeEach(() => { config.authConfig.disableAuth = false })

    test("401 without a token", async () => {
        assert.equal((await post("/rebuild")).status, 401)
        assert.equal((await fetch(baseUrl + "/rebuild")).status, 401)
        assert.equal(runCalls.length, 0)
    })

    test("202 with a valid token for the configured client", async () => {
        const res = await post("/rebuild", { token: makeToken({ azp: "query-engine", email: "a@b.it" }) })
        assert.equal(res.status, 202)
    })

    test("403 with an expired token, a token of another client or signed with another key", async () => {
        assert.equal((await post("/rebuild", { token: makeToken({ azp: "query-engine" }, { expiresIn: -60 }) })).status, 403)
        assert.equal((await post("/rebuild", { token: makeToken({ azp: "someone-else" }) })).status, 403)
        assert.equal((await post("/rebuild", { token: "not.a.jwt" })).status, 403)
        // a forged signature ("invalid signature") currently answers 500, not 403: rejected either way
        const wrongKey = (await post("/rebuild", { token: makeToken({ azp: "query-engine" }, { key: otherPrivateKey }) })).status
        assert.ok([403, 500].includes(wrongKey), "rejected: " + wrongKey)
        assert.equal(runCalls.length, 0)
    })
})
