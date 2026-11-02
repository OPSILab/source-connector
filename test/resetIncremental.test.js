// The reset of the incremental APIs (utils/resetIncremental.js): their records deleted (MongoDB, PostgreSQL,
// keys/values/entries, pending polls) and polled again from startDateValue, on real MongoDB + PostgreSQL (see
// helpers/db.js), against a fake API.
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const { loadApiConnector, fakePagedApi } = require("./helpers/apiConnector")

const INCR = "https://api.example/incremental"
const PLAIN = "https://api.example/plain"
const DAY = 1000 * 60 * 60 * 24
const ymd = time => new Date(time).toISOString().split("T")[0]
const daysAgo = days => ymd(Date.now() - days * DAY)

let pollAPI, records, Source, Entries, locks, reset, fake

before(async () => {
    await db.setup(__filename)
    records = load("utils/sourceRecords.js")
    Source = load("utils/collections.js").collectionModel("api")
    Entries = load("api/models/Entries.js")
    locks = load("utils/jobLocks.js")
    reset = load("utils/resetIncremental.js")
    await load("utils/pollStatus.js").ensurePgStatusTable()
    pollAPI = await loadApiConnector()
})
after(db.teardown)
afterEach(() => fake?.restore())

const incremental = (extra = {}) => ({
    name: "Incremental", url: INCR, incremental: true,
    pagination: { offsetParam: "offset", limitParam: "limit", limit: 2, condition: response => response.data.length > 0 },
    incrementalParams: { startDateParam: "from", startDateFormat: "YYYY-MM-DD", startDateValue: daysAgo(30), endDateParam: "to", endDateFormat: "YYYY-MM-DD", endDateLogic: "inclusive", maxWindow: 10 * DAY },
    ...extra
})
const plain = { name: "Plain", url: PLAIN }

// The records of the last days, polled twice: duplicated
async function storeDuplicated() {
    const { createCollector } = load("utils/entriesStore.js")
    const collector = createCollector(INCR, "api"), plainCollector = createCollector(PLAIN, "api")
    const item = (id, v) => records.makeItem({ id, v, datePolled: new Date(Date.now() - DAY) }, INCR)
    await records.storeRecords([item(1, "old"), item(1, "old"), item(2, "dup")], INCR, { collector })
    await records.storeRecords([records.makeItem({ id: 9, v: "plain" }, PLAIN)], PLAIN, { collector: plainCollector })
    await collector.flush()
    await plainCollector.flush()
}

beforeEach(async () => {
    await db.clean()
    await db.query("TRUNCATE status RESTART IDENTITY")
    locks.polling = locks.rebuilding = false
    config.apiConnectorConfig.apiUrls = [incremental(), plain]
    await storeDuplicated()
})

const pgOf = async url => (await db.query("SELECT data FROM sources WHERE record->>'from' = $1", [url])).map(row => row.data.sourceId).sort()

describe("resetIncremental", () => {
    test("the incremental API from scratch: its records deleted, polled again from startDateValue in windows", async () => {
        fake = fakePagedApi([[{ id: 1, v: "new" }, { id: 2, v: "new" }]])
        const { apis } = await reset.resetIncremental({ pollAPI })
        assert.deepEqual(apis.map(api => [api.name, api.before, api.after]), [["Incremental", { mongo: 3, postgres: 3 }, { mongo: 6, postgres: 6 }]])
        // three windows of 10 days from startDateValue, each one from its first page
        assert.deepEqual(fake.requests.filter(r => !r.offset).map(r => [r.params.from, r.params.to]),
            [[daysAgo(30), daysAgo(21)], [daysAgo(20), daysAgo(11)], [daysAgo(10), daysAgo(1)]])
        assert.deepEqual((await db.docs(Source, { source: INCR })).map(doc => doc.v), ["new", "new", "new", "new", "new", "new"])
        assert.deepEqual(await pgOf(INCR), [1, 1, 1, 2, 2, 2])
        // the other API untouched; the values of the deleted records gone
        assert.deepEqual((await db.docs(Source, { source: PLAIN })).map(doc => doc.v), ["plain"])
        assert.deepEqual(await pgOf(PLAIN), [9])
        const values = async url => (await Entries.collection.find({ key: "v", "refs.origin": url }).toArray()).map(doc => doc.value).sort()
        assert.deepEqual(await values(INCR), ["new"])
        assert.deepEqual(await values(PLAIN), ["plain"])
        assert.equal(locks.polling || locks.rebuilding, false)
        assert.equal(reset.getResetStatus().phase, "done")
    })

    test("dry run: only the counts", async () => {
        fake = fakePagedApi([])
        const result = await reset.resetIncremental({ dryRun: true })
        assert.deepEqual(result, { dryRun: true, apis: [{ name: "Incremental", url: INCR, records: { mongo: 3, postgres: 3 } }] })
        assert.equal((await db.docs(Source, { source: INCR })).length, 3)
        assert.deepEqual(fake.requests, [])
    })

    test("only the APIs named; an unknown or not incremental name: an error, nothing deleted", async () => {
        config.apiConnectorConfig.apiUrls = [incremental(), incremental({ name: "Other", url: "https://api.example/other" }), plain]
        assert.deepEqual(reset.selectApis(["Other"]).map(api => api.name), ["Other"])
        assert.deepEqual(reset.selectApis([]).map(api => api.name), ["Incremental", "Other"])
        await assert.rejects(reset.resetIncremental({ names: ["Plain"], pollAPI }), /Not incremental APIs.*Plain/)
        await assert.rejects(reset.resetIncremental({ names: ["Nope"], pollAPI }), /Nope/)
        assert.equal((await db.docs(Source, { source: INCR })).length, 3)
    })

    test("refused while a poll or a rebuild runs", async () => {
        locks.polling = true
        await assert.rejects(reset.resetIncremental({ pollAPI }), /poll is running/)
        locks.polling = false
        locks.rebuilding = true
        await assert.rejects(reset.resetIncremental({ pollAPI }), /rebuild is running/)
        locks.rebuilding = false
        assert.equal((await db.docs(Source, { source: INCR })).length, 3)
    })

    test("not paginated: polled again in windows too", async () => {
        config.apiConnectorConfig.apiUrls = [incremental({ pagination: undefined }), plain]
        fake = fakePagedApi([[{ id: 1, v: "new" }]])
        const { apis } = await reset.resetIncremental({ pollAPI })
        assert.equal(fake.requests.length, 3)
        assert.deepEqual(apis[0].after, { mongo: 3, postgres: 3 })
    })
})
