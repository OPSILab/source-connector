// Paginated API polls interrupted before their last page (utils/pollStatus.js, apiConnector) on real MongoDB +
// PostgreSQL (see helpers/db.js: npm run test:db). While a poll runs, every record stored by a page is referenced in
// the status collection / table with pending: true; the next poll deletes the records left pending by an incomplete
// poll and starts again.
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const { loadApiConnector, fakePagedApi } = require("./helpers/apiConnector")

const URL_A = "https://api.example/records"
const URL_B = "https://api.example/other"
const DAY = new Date("2026-10-22T08:00:00Z")

let records, pollStatus, Source, Status, Entries, createCollector

before(async () => {
    await db.setup(__filename)
    records = load("utils/sourceRecords.js")
    pollStatus = load("utils/pollStatus.js")
    Source = load("utils/collections.js").collectionModel("api")
    Status = load("api/models/Status.js")
    Entries = load("api/models/Entries.js")
    createCollector = load("utils/entriesStore.js").createCollector
    await pollStatus.ensurePgStatusTable()
})
after(db.teardown)
beforeEach(async () => {
    await db.clean()
    await db.query("TRUNCATE status RESTART IDENTITY")
})

const statusDocs = () => db.docs(Status)
const pgStatus = () => db.query("SELECT record_id, record_table, url, pending, date_polled FROM status ORDER BY id")
const pgSources = () => db.query("SELECT id, data FROM sources ORDER BY id")

// Stores `items` of `url` as a page of a poll (keys/values/entries too) and returns what storeRecords returned
async function storePage(items, url) {
    const collector = createCollector(url, "api")
    const stored = await records.storeRecords(items.map(item => records.makeItem({ ...item }, url)), url, { collector })
    await collector.flush()
    return stored
}

describe("storeRecords", () => {
    test("returns the _id of the MongoDB documents and the id of the PostgreSQL rows", async () => {
        const stored = await storePage([{ id: 1, v: "a" }, { id: 2, v: "b" }], URL_A)
        const mongo = await Source.collection.find({}).toArray()
        assert.deepEqual(stored.mongo.map(String).sort(), mongo.map(doc => String(doc._id)).sort())
        assert.deepEqual(stored.postgres, (await pgSources()).map(row => row.id))
    })

    test("nothing to store: empty arrays", async () => {
        assert.deepEqual(await records.storeRecords([], URL_A), { mongo: [], postgres: [] })
    })
})

describe("markPending / completePoll", () => {
    test("every stored record referenced with url, pending and datePolled; completePoll deletes only that poll", async () => {
        const first = await storePage([{ id: 1 }, { id: 2 }], URL_A)
        await pollStatus.markPending(URL_A, DAY, first)
        const other = await storePage([{ id: 3 }], URL_B)
        await pollStatus.markPending(URL_B, DAY, other)

        const mongo = await Status.collection.find({ url: URL_A }).toArray()
        assert.deepEqual(mongo.map(doc => String(doc._id)).sort(), first.mongo.map(String).sort())
        assert.ok(mongo.every(doc => doc.pending === true && new Date(doc.datePolled).getTime() == DAY.getTime()))
        const pg = (await pgStatus()).filter(row => row.url == URL_A)
        assert.deepEqual(pg.map(row => row.record_id), first.postgres)
        assert.ok(pg.every(row => row.pending && row.record_table == "sources" && row.date_polled.getTime() == DAY.getTime()))

        await pollStatus.completePoll(URL_A, DAY)
        assert.deepEqual((await statusDocs()).map(doc => doc.url), [URL_B])
        assert.deepEqual((await pgStatus()).map(row => row.url), [URL_B])
    })
})

describe("rollbackPending", () => {
    test("deletes the pending records of the url (MongoDB and PostgreSQL) and their references, keeps the others", async () => {
        await storePage([{ id: 1, v: "complete" }], URL_A)                               // a complete poll
        const pending = await storePage([{ id: 2, v: "partial" }, { id: 3, v: "partial" }], URL_A)
        await pollStatus.markPending(URL_A, DAY, pending)
        const otherApi = await storePage([{ id: 9, v: "other" }], URL_B)
        await pollStatus.markPending(URL_B, DAY, otherApi)

        assert.deepEqual(await pollStatus.rollbackPending(URL_A), { mongo: 2, postgres: 2 })

        assert.deepEqual((await db.docs(Source, { source: URL_A })).map(doc => doc.sourceId), [1])
        assert.deepEqual((await pgSources()).map(row => row.data.sourceId), [1, 9])
        assert.deepEqual((await statusDocs()).map(doc => doc.url), [URL_B])
        assert.deepEqual((await pgStatus()).map(row => row.url), [URL_B])
        // keys/values/entries of URL_A rebuilt from what remains: the values of the deleted records are gone
        const valuesOfA = (await Entries.collection.find({ key: "v", "refs.origin": URL_A }).toArray()).map(doc => doc.value)
        assert.deepEqual(valuesOfA, ["complete"])
    })

    test("nothing pending: nothing deleted", async () => {
        await storePage([{ id: 1 }], URL_A)
        assert.deepEqual(await pollStatus.rollbackPending(URL_A), { mongo: 0, postgres: 0 })
        assert.equal((await db.docs(Source)).length, 1)
    })
})

describe("apiConnector: a paginated poll interrupted and repeated", () => {
    let pollAPI, fake
    const PAGES = [[{ id: 1, v: "a" }, { id: 2, v: "b" }], [{ id: 3, v: "c" }, { id: 4, v: "d" }], [{ id: 5, v: "e" }]]

    before(async () => { pollAPI = await loadApiConnector() })
    beforeEach(() => { fake = fakePagedApi(PAGES) })
    afterEach(() => fake.restore())

    const api = incremental => ({
        name: "Paged",
        url: URL_A,
        pagination: { offsetParam: "offset", limitParam: "limit", limit: 2, offset: 0, condition: response => response.data.length > 0 },
        ...(incremental ? {
            incremental: true,
            incrementalParams: { startDateParam: "from", startDateFormat: "YYYY-MM-DD", startDateValue: "2026-01-01", endDateParam: "to", endDateFormat: "YYYY-MM-DD" }
        } : {})
    })

    for (const incremental of [true, false])
        test(`${incremental ? "incremental" : "not incremental"}: the records of the interrupted poll are deleted, the next poll stores every page once`, async () => {
            config.apiConnectorConfig.apiUrls = [api(incremental)]

            fake.failAtOffset = 2 // the second page fails
            await pollAPI()
            assert.deepEqual((await db.docs(Source)).map(doc => doc.sourceId), [1, 2])
            assert.equal((await statusDocs()).filter(doc => doc.pending).length, 2)
            assert.equal((await pgStatus()).length, 2)

            fake.failAtOffset = undefined
            fake.requests.length = 0
            await pollAPI()
            assert.deepEqual(fake.requests.map(r => r.offset), [0, 2, 4, 6]) // polled again from the first page
            if (incremental)
                assert.equal(fake.requests[0].params.from, "2026-01-01") // the interrupted poll does not count as polled
            assert.deepEqual((await db.docs(Source)).map(doc => doc.sourceId).sort(), [1, 2, 3, 4, 5])
            assert.deepEqual((await pgSources()).map(row => row.data.sourceId).sort(), [1, 2, 3, 4, 5])
            assert.deepEqual(await statusDocs(), [])
            assert.deepEqual(await pgStatus(), [])
        })

    test("a complete poll leaves nothing in status", async () => {
        config.apiConnectorConfig.apiUrls = [api(true)]
        await pollAPI()
        assert.equal((await db.docs(Source)).length, 5)
        assert.deepEqual(await statusDocs(), [])
        assert.deepEqual(await pgStatus(), [])
    })
})
