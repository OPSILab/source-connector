// Incremental API polls when the API records are not stored in MongoDB (collections.api.toMongo false): the last
// poll is read from PostgreSQL (max datePolled of the url's rows), on real PostgreSQL (see helpers/db.js).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const { loadApiConnector, fakePagedApi } = require("./helpers/apiConnector")

const URL_A = "https://api.example/records"
const DAY = 1000 * 60 * 60 * 24
const ymd = date => date.toISOString().split("T")[0]

let pollAPI, fake, records, Source

before(async () => {
    await db.setup(__filename)
    records = load("utils/sourceRecords.js")
    Source = load("utils/collections.js").collectionModel("api")
    pollAPI = await loadApiConnector()
})
after(db.teardown)
beforeEach(async () => {
    await db.clean()
    await db.query("CREATE TABLE IF NOT EXISTS status (id SERIAL PRIMARY KEY, record_id INTEGER NOT NULL, record_table TEXT NOT NULL, url TEXT NOT NULL, pending BOOLEAN NOT NULL DEFAULT true, date_polled TIMESTAMPTZ NOT NULL)")
    await db.query("TRUNCATE status RESTART IDENTITY")
    config.collections.api.toMongo = false
    fake = fakePagedApi([[{ id: 1 }, { id: 2 }], [{ id: 3 }]])
})
afterEach(() => fake.restore())

const incrementalApi = {
    name: "Incremental",
    url: URL_A,
    pagination: { offsetParam: "offset", limitParam: "limit", limit: 2, offset: 0, condition: response => response.data.length > 0 },
    incremental: true,
    incrementalParams: { startDateParam: "from", startDateFormat: "YYYY-MM-DD", startDateValue: "2026-01-01", endDateParam: "to", endDateFormat: "YYYY-MM-DD" }
}

const pgRows = () => db.query("SELECT data FROM sources ORDER BY id")

describe("incremental API, records only in PostgreSQL", () => {
    test("first poll from startDateValue; a second poll the same day is skipped (the last poll read from PostgreSQL)", async () => {
        config.apiConnectorConfig.apiUrls = [incrementalApi]
        await pollAPI()
        assert.equal(fake.requests[0].params.from, "2026-01-01")
        assert.equal((await pgRows()).length, 3)
        assert.equal((await db.docs(Source)).length, 0) // nothing in MongoDB

        fake.requests.length = 0
        await pollAPI()
        assert.deepEqual(fake.requests, [])
        assert.equal((await pgRows()).length, 3)
    })

    test("last poll two days ago: the poll starts from that day", async () => {
        const lastPoll = new Date(Date.now() - 2 * DAY)
        await records.storeRecords([records.makeItem({ id: 0, datePolled: lastPoll }, URL_A)], URL_A)
        config.apiConnectorConfig.apiUrls = [incrementalApi]
        await pollAPI()
        assert.equal(fake.requests[0].params.from, ymd(lastPoll))
        assert.equal(fake.requests[0].params.to, ymd(new Date()))
        assert.equal((await pgRows()).length, 4)
    })

    test("other urls and rows without datePolled are not taken into account", async () => {
        await records.storeRecords([records.makeItem({ id: 0, datePolled: new Date() }, "https://api.example/other")], "https://api.example/other")
        await records.storeRecords([records.makeItem({ id: 0 }, URL_A)], URL_A)
        config.apiConnectorConfig.apiUrls = [incrementalApi]
        await pollAPI()
        assert.equal(fake.requests[0].params.from, "2026-01-01")
    })

    test("records stored neither in MongoDB nor in PostgreSQL: every poll from startDateValue", async () => {
        config.collections.api.toPostgres = false
        config.apiConnectorConfig.apiUrls = [incrementalApi]
        await pollAPI()
        fake.requests.length = 0
        await pollAPI()
        assert.equal(fake.requests[0].params.from, "2026-01-01")
    })
})
