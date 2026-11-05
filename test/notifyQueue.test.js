// Orion notifications (POST /api/orion/subscribe/:mapID, through the real router, controller and service) while the
// Data Model Mapper fails in different ways: the notification queue (utils/requestQueue.js, used by
// service.notifyPath) must take the notifications one at a time, in arrival order, and go on after every failure.
// Orion is the sender (the notifications are the ones it posts), the mapper is a local HTTP server; Keycloak, the
// entities collection, the datapoints writer and the connectors are replaced by stubs. No database needed.
const { stub, load, config, resetConfig } = require("./helpers/env")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const http = require("http")
const express = require("express")

// The entities collection: findOne by entityId / by downloadURL (dotted paths, exact values)
let entities = []
const valueAt = (doc, path) => path.split(".").reduce((value, key) => value?.[key], doc)
stub("api/models/Entity.js", {
    findOne: async filter => structuredClone(entities.find(doc => Object.entries(filter).every(([path, value]) => valueAt(doc, path) === value)) || null),
    findOneAndUpdate: async () => { },
    insertMany: async docs => { entities.push(...structuredClone(docs)) }
})
stub("api/models/Dimensions.js", { findOne: async () => null, findOneAndUpdate: async () => { } })
// Keycloak: token-1 at start, a new one at every refresh
const refreshes = []
stub("utils/keycloak.js", {
    updateJWT: async force => {
        if (force)
            refreshes.push(force)
        return "token-" + (refreshes.length + 1)
    }
})
// the datapoints of each dataset (by url)
const written = {}
stub("utils/datapoints.js", {
    upsertDatapoints: async () => { throw new Error("upsertDatapoints not expected (a mapID is given)") },
    openDatapointsWriter: async url => ({ add: async rows => { written[url] = [...(written[url] || []), ...rows] }, close: async () => { } })
})
stub("utils/sourceRecords.js", { replaceRecords: async () => { throw new Error("replaceRecords not expected") } })
stub("inputConnectors/minioConnector.js", {})
stub("inputConnectors/orionConnector.js", {})
stub("inputConnectors/apiConnector.js", { pollAPI: async () => { } })

config.orion.attrWithUrl = "downloadURL" // read by service.js when it is loaded
const router = load("api/routes/router.js")
const service = load("api/services/service.js")

const MAP_ID = "map-1"
const DATASET = name => "https://data.example/sdmx/" + name

// ---- the Data Model Mapper ----
// plans[dataset url]: the answers to its POST /map/transform (or /parse), in order: an HTTP status, "ok" (a session
// with one part of datapoints) or "session-error" (a session that ends with an error).
// mapPlans[dataset url]: the answers to its GET /map, in order (an HTTP status; none / "ok": the map)
let plans, mapPlans, sessions, mapperLog, gate, mapper, mapperUrl
// holds[dataset url]: a promise its GET /map waits for (the notification stays the one being processed meanwhile)
let holds = {}

function startMapper() {
    mapper = http.createServer(async (req, res) => {
        const url = new URL(req.url, "http://localhost")
        const auth = req.headers.authorization
        const send = (status, body) => {
            res.statusCode = status
            res.setHeader("Content-Type", "application/json")
            res.end(JSON.stringify(body))
        }
        let body = ""
        for await (const chunk of req)
            body += chunk
        if (url.pathname == "/api/map") {
            const dataset = url.searchParams.get("description")
            const answer = mapPlans[dataset]?.shift() ?? "ok"
            mapperLog.push({ dataset, call: "map", answer, auth })
            await gate // the first notification waits here until every notification has arrived
            await holds[dataset]
            if (typeof answer == "number")
                return send(answer, { message: "map: " + answer })
            return send(200, { _id: MAP_ID, description: dataset })
        }
        if (url.pathname == "/api/map/transform" || url.pathname == "/api/parse") {
            const dataset = JSON.parse(body).sourceDataURL
            const answer = plans[dataset].shift()
            mapperLog.push({ dataset, call: url.pathname == "/api/parse" ? "parse" : "transform", answer, auth })
            if (typeof answer == "number")
                return send(answer, { message: "refused with " + answer })
            const id = "session-" + Object.keys(sessions).length
            sessions[id] = answer == "ok"
                ? { dataset, parts: [[{ _id: 1, survey: "S1", dimensions: { geo: "IT" }, value: 7 }]], status: "done" }
                : { dataset, parts: [], status: "error", error: "mapping failed at row 3" }
            return send(200, { id })
        }
        const session = sessions[url.searchParams.get("id")]
        mapperLog.push({ dataset: session?.dataset, call: url.pathname.split("/").pop() })
        if (!session)
            return send(404, { message: "unknown session" })
        if (url.pathname == "/api/output")
            return send(200, session.parts[Number(url.searchParams.get("index"))] || [])
        if (url.pathname == "/api/feedback")
            return send(200, "OK")
        if (url.pathname == "/api/reportSync")
            return send(200, { completed: true, status: session.status, error: session.error, parts: session.parts.length })
        send(404, {})
    })
    return new Promise(resolve => mapper.listen(0, "127.0.0.1", () => resolve(`http://127.0.0.1:${mapper.address().port}/api`)))
}

let server, baseUrl
let arrived = 0 // notifications received (and queued: the route queues them synchronously)

before(async () => {
    mapperUrl = await startMapper()
    const app = express()
    app.use(express.json())
    app.use((req, res, next) => {
        next()
        if (req.originalUrl.startsWith("/api/orion/subscribe/"))
            arrived++
    })
    app.use("/api", router)
    await new Promise(resolve => { server = app.listen(0, "127.0.0.1", resolve) })
    baseUrl = `http://127.0.0.1:${server.address().port}/api`
    await new Promise(resolve => setImmediate(resolve)) // the Keycloak token of service.js
})
after(() => {
    server?.close()
    mapper?.close()
})

beforeEach(() => {
    resetConfig()
    Object.assign(config, {
        getMapEndpoint: mapperUrl + "/map", mapEndpoint: mapperUrl + "/map/transform", parseEndpoint: mapperUrl + "/parse",
        sessionEndpoint: mapperUrl + "/output?", reportEndpoint: mapperUrl + "/reportSync", feedbackEndpoint: mapperUrl + "/feedback"
    })
    config.orion.attrWithUrl = "downloadURL"
    config.authConfig.disableAuth = true
    entities = []
    mapPlans = {}
    holds = {}
    sessions = {}
    mapperLog = []
    refreshes.length = 0
    for (const key of Object.keys(written))
        delete written[key]
})

// A notification of Orion: one SDMX distribution
const notification = name => ({
    subscriptionId: "urn:ngsi-ld:Subscription:1",
    data: [{ id: "urn:ngsi-ld:Distribution:" + name, type: "DistributionDCAT-AP", format: { type: "Property", value: "xml" }, downloadURL: { type: "Property", value: DATASET(name) } }]
})
const notify = body => fetch(`${baseUrl}/orion/subscribe/${MAP_ID}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
    .then(async res => ({ status: res.status, text: await res.text() }))

// Posts the notifications one after the other while the first one to reach the mapper is held there (each one queued
// before the next is sent: their order is the arrival order), then (after onHeld, if any) lets the mapper answer.
// The HTTP answers.
async function notifyAll(bodies, onHeld) {
    let open
    gate = new Promise(resolve => { open = resolve })
    const answers = []
    const start = arrived
    for (const [i, body] of bodies.entries()) {
        answers.push(notify(body))
        while (arrived < start + i + 1)
            await new Promise(resolve => setTimeout(resolve, 5))
    }
    if (onHeld)
        await onHeld()
    open()
    return Promise.all(answers)
}

// The datasets in the order the mapper saw them, each one in a single block (never two at the same time)
function blocks() {
    const order = []
    for (const { dataset } of mapperLog)
        if (order[order.length - 1] !== dataset)
            order.push(dataset)
    assert.equal(new Set(order).size, order.length, "the requests of two notifications are interleaved: " + order.join(", "))
    return order
}

describe("Orion notifications while the mapper fails", () => {
    test("token refused (401 / 403), 500, a failed session, map errors: every notification handled in order, the queue goes on", async () => {
        plans = {
            [DATASET("expired-401")]: [401, "ok"],      // token expired (standard answer): refreshed, retried
            [DATASET("refused")]: [401, 401],           // token always refused: given up after the second attempt
            [DATASET("broken")]: [500],                 // mapper error: not retried
            [DATASET("failing-session")]: ["session-error"],
            [DATASET("expired-403")]: [403, "ok"],      // token expired (mapper not aligned yet): refreshed, retried
            [DATASET("map-down")]: [],                  // GET /map 500: neither mapped nor parsed
            [DATASET("map-expired")]: ["ok"],           // GET /map 401: token refreshed, then mapped
            [DATASET("no-map")]: ["ok"],                // GET /map 404: parsed
            [DATASET("fine")]: ["ok"]
        }
        mapPlans = { [DATASET("map-down")]: [500], [DATASET("map-expired")]: [401], [DATASET("no-map")]: [404] }
        const names = ["expired-401", "refused", "broken", "failing-session", "expired-403", "map-down", "map-expired", "no-map", "fine"]
        const answers = await notifyAll(names.map(notification))

        // every notification answered (the errors are handled inside executeRequest: "OK")
        assert.deepEqual(answers, names.map(() => ({ status: 200, text: "OK" })))
        assert.equal(service.queue(), 0)
        // one at a time, in arrival order
        assert.deepEqual(blocks(), names.map(DATASET))
        // what the mapper was asked for each dataset
        const calls = name => mapperLog.filter(entry => entry.dataset == DATASET(name) && ["map", "transform", "parse"].includes(entry.call))
            .map(entry => entry.call + ":" + entry.answer)
        assert.deepEqual(names.map(name => [name, calls(name)]), [
            ["expired-401", ["map:ok", "transform:401", "map:ok", "transform:ok"]],
            ["refused", ["map:ok", "transform:401", "map:ok", "transform:401"]],
            ["broken", ["map:ok", "transform:500"]],
            ["failing-session", ["map:ok", "transform:session-error"]],
            ["expired-403", ["map:ok", "transform:403", "map:ok", "transform:ok"]],
            ["map-down", ["map:500"]],
            ["map-expired", ["map:401", "map:ok", "transform:ok"]],
            ["no-map", ["map:404", "parse:ok"]],
            ["fine", ["map:ok", "transform:ok"]]])
        // a refresh for every refused token, and the next request with the new one
        assert.equal(refreshes.length, 5) // expired-401, refused (twice), expired-403, map-expired
        const retried = mapperLog.filter(entry => entry.dataset == DATASET("expired-401") && entry.call == "transform")
        assert.notEqual(retried[0].auth, retried[1].auth)
        // written and stored only when the mapping succeeded: the failed ones are mapped again at the next notification
        const ok = ["expired-401", "expired-403", "map-expired", "no-map", "fine"]
        assert.deepEqual(Object.keys(written).sort(), ok.map(DATASET).sort())
        assert.deepEqual(written[DATASET("fine")].map(row => [row.value, row.fromUrl]), [[7, DATASET("fine")]])
        assert.deepEqual(entities.map(doc => doc.entityId).sort(), ok.map(name => "urn:ngsi-ld:Distribution:" + name).sort())
    })

    test("GET /api/queue: how many, the one being processed (its datasets, since when), the others in arrival order", async () => {
        plans = { [DATASET("one")]: ["ok"], [DATASET("two")]: ["ok"], [DATASET("three")]: ["ok"] }
        let held
        await notifyAll(["one", "two", "three"].map(notification), async () => {
            held = await (await fetch(`${baseUrl}/queue`)).json()
        })
        assert.equal(held.queue, 3)
        assert.deepEqual(held.processing.entities, [{ id: "urn:ngsi-ld:Distribution:one", url: DATASET("one") }])
        assert.ok(held.processing.startedAt && held.processing.receivedAt)
        assert.deepEqual(held.waiting.map(job => job.entities[0].url), [DATASET("two"), DATASET("three")])
        assert.ok(held.waiting.every(job => job.receivedAt && !job.startedAt))
        assert.deepEqual(await (await fetch(`${baseUrl}/queue`)).json(), { queue: 0, processing: null, waiting: [] })
    })

    test("GET /api/queue step by step: 3, 2, 1, 0 as each notification ends, the next one processed each time", async () => {
        const names = ["one", "two", "three"]
        plans = Object.fromEntries(names.map(name => [DATASET(name), ["ok"]]))
        const release = {}
        for (const name of names)
            holds[DATASET(name)] = new Promise(resolve => { release[name] = resolve })
        const queue = async () => (await fetch(`${baseUrl}/queue`)).json()
        const waitFor = async condition => {
            for (const started = Date.now(); !(await condition());)
                if (Date.now() - started > 5000)
                    throw new Error("timed out")
                else
                    await new Promise(resolve => setTimeout(resolve, 10))
        }
        const before = arrived
        const done = notifyAll(names.map(notification))
        await waitFor(() => arrived >= before + names.length) // all three in the queue
        for (const [i, name] of names.entries()) {
            // the notification is at its GET /map, held: it is the one being processed
            await waitFor(() => mapperLog.some(entry => entry.call == "map" && entry.dataset == DATASET(name)))
            const state = await queue()
            assert.equal(state.queue, names.length - i, name)
            assert.equal(state.processing.entities[0].url, DATASET(name))
            assert.deepEqual(state.waiting.map(job => job.entities[0].url), names.slice(i + 1).map(DATASET))
            release[name]()
        }
        assert.deepEqual(await done, names.map(() => ({ status: 200, text: "OK" })))
        assert.deepEqual(await queue(), { queue: 0, processing: null, waiting: [] })
        assert.deepEqual(Object.keys(written).sort(), names.map(DATASET).sort())
    })

    test("a notification that throws (an invalid url): 500 for it only, the next one is mapped", async () => {
        plans = { [DATASET("after")]: ["ok"] }
        const invalid = notification("invalid")
        invalid.data[0].downloadURL.value = "not a url"
        const answers = await notifyAll([invalid, notification("after")])
        assert.equal(answers[0].status, 500)
        assert.match(answers[0].text, /Invalid URL/)
        assert.deepEqual(answers[1], { status: 200, text: "OK" })
        assert.equal(service.queue(), 0)
        assert.deepEqual(Object.keys(written), [DATASET("after")])
    })

    test("the same failing dataset notified again: mapped again (not stored after the failure)", async () => {
        plans = { [DATASET("retry")]: [500, "ok"] }
        await notifyAll([notification("retry")])
        assert.deepEqual(entities, [])
        const [answer] = await notifyAll([notification("retry")])
        assert.deepEqual(answer, { status: 200, text: "OK" })
        assert.deepEqual(written[DATASET("retry")].map(row => row.value), [7])
        assert.deepEqual(entities.map(doc => doc.entityId), ["urn:ngsi-ld:Distribution:retry"])
    })
})
