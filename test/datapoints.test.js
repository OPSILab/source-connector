// Orion datapoints (utils/datapoints.js) in the Orion collection, on real MongoDB + PostgreSQL (npm run test:db).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

let datapoints, Orion, Api, Entries, Values, Key

before(async () => {
    await db.setup(__filename)
    datapoints = load("utils/datapoints.js")
    const collections = load("utils/collections.js")
    Orion = collections.collectionModel("orion")
    Api = collections.collectionModel("api")
    Entries = load("api/models/Entries.js")
    Values = load("api/models/Value.js")
    Key = load("api/models/Key.js")
})
after(db.teardown)
beforeEach(db.clean)

const URL_A = "https://ec.europa.eu/eurostat/api/nama_10r_3gdp.xml"
// as the data model mapper sends them (with its own _id)
const mapped = (n, extra = {}) => ({
    _id: "mapper-id-" + n, source: "EUROSTAT", survey: "nama_10r.3gdp", region: "LOVECH",
    dimensions: ["Lovech", "Euro per inhabitant", String(2019 + n)], value: 1000 + n, timestamp: "2020-01-01", ...extra
})
const pgRows = () => db.query("SELECT name, data, record FROM datapoints ORDER BY id")

describe("upsertDatapoints", () => {
    test("in the Orion collection (datapoints), legacy shape: source = provider, fromUrl = dataset, dupl_hash", async () => {
        const count = await datapoints.upsertDatapoints([mapped(1), mapped(2)], URL_A)
        assert.equal(count, 2)
        const docs = await db.docs(Orion)
        assert.equal(docs.length, 2)
        assert.equal(Orion.collection.collectionName, "datapoints")
        for (const doc of docs) {
            assert.equal(doc.source, "EUROSTAT")
            assert.equal(doc.fromUrl, URL_A)
            assert.equal(doc.survey, "NAMA_10R3GDP") // cleanSurveyName, as before
            assert.equal(typeof doc.dupl_hash, "string")
            assert.ok(!("sourceId" in doc))
        }
        assert.ok(!(await Orion.collection.find({}).toArray()).some(doc => String(doc._id).startsWith("mapper-id")))
        assert.equal((await db.docs(Api)).length, 0) // nothing in the API collection
    })

    test("upsert by dupl_hash: the same datapoint twice is stored once, nothing deleted when the dataset changes", async () => {
        await datapoints.upsertDatapoints([mapped(1), mapped(2)], URL_A)
        await datapoints.upsertDatapoints([mapped(2), mapped(3)], URL_A)
        assert.deepEqual((await db.docs(Orion)).map(d => d.value).sort(), [1001, 1002, 1003])
    })

    test("indexed by dimension, with the orion connector; `value` not indexed (orion.datapointsNotIndexed)", async () => {
        await datapoints.upsertDatapoints([mapped(1), mapped(2)], URL_A)
        const dims = await db.docs(Entries, { key: "dimensions" })
        assert.deepEqual(dims.map(e => e.value).sort(), ["2020", "2021", "Euro per inhabitant", "Lovech"])
        assert.deepEqual(dims[0].connectors, ["orion"])
        assert.deepEqual(dims[0].refs, [{ origin: URL_A, visibility: "public-data", connector: "orion", format: "object" }])
        assert.deepEqual(await db.docs(Entries, { key: "value" }), [])
        assert.deepEqual((await db.docs(Key, { key: "value" }))[0].valuesNotIndexed, [URL_A])
        assert.deepEqual(await db.docs(Entries, { key: "sourceId" }), [])
        assert.equal((await db.docs(Values, { value: "Lovech" }))[0].refs[0].origin, URL_A)
    })

    test("not copied to PostgreSQL by default", async () => {
        await datapoints.upsertDatapoints([mapped(1)], URL_A)
        assert.deepEqual(await pgRows(), [])
    })

    test("collections.orion.toPostgres: the dataset's rows replaced in its table", async () => {
        config.collections.orion.toPostgres = true
        await datapoints.upsertDatapoints([mapped(1), mapped(2)], URL_A, { sqlName: "nama" })
        await datapoints.upsertDatapoints([mapped(3)], URL_A, { sqlName: "nama" })
        const rows = await pgRows()
        assert.deepEqual(rows.map(r => r.data.value), [1003])
        assert.deepEqual(rows[0].record, { from: URL_A })
    })

    test("collections.orion.toMongo = false: nothing in MongoDB, no keys / values / entries", async () => {
        config.collections.orion.toMongo = false
        config.collections.orion.toPostgres = true
        await datapoints.upsertDatapoints([mapped(1)], URL_A)
        assert.deepEqual(await db.docs(Orion), [])
        assert.deepEqual(await db.docs(Entries), [])
        assert.equal((await pgRows()).length, 1)
    })

    test("block by block (mapper chunks)", async () => {
        const writer = await datapoints.openDatapointsWriter(URL_A)
        await writer.add([mapped(1)])
        await writer.add([])
        await writer.add([mapped(2), mapped(3)])
        assert.equal(await writer.close(), 3)
        assert.equal((await db.docs(Orion)).length, 3)
    })

    test("a custom collection name", async () => {
        config.collections.orion.mongo = "orion_data"
        const Custom = load("utils/collections.js").collectionModel("orion")
        await datapoints.upsertDatapoints([mapped(1)], URL_A)
        assert.equal(Custom.collection.collectionName, "orion_data")
        assert.equal((await db.docs(Custom)).length, 1)
        assert.equal((await db.docs(Orion)).length, 0)
        await Custom.collection.deleteMany({})
    })
})

describe("without upsertRecords (insert; the mapper flow empties the survey first)", () => {
    const URL_B = "https://ec.europa.eu/eurostat/api/demo_r_gind3.xml"
    beforeEach(async () => {
        config.upsertRecords = false
        // as in production with upsertRecords false: no unique index on dupl_hash (the tests' schema made one)
        await Orion.init?.()
        await Orion.collection.dropIndex?.("dupl_hash_1").catch(() => { })
    })

    test("replaceSurvey: the survey is emptied at the first block, then filled; its old values leave the suggestions", async () => {
        await datapoints.upsertDatapoints([mapped(1), mapped(2)], URL_A, { replaceSurvey: true })
        await datapoints.upsertDatapoints([{ ...mapped(9), survey: "demo_r_gind3", dimensions: ["Sofia"] }], URL_B, { replaceSurvey: true })
        // a datapoint written by the old code: survey uppercase only (insertMany), no fromUrl
        await Orion.collection.insertOne({ source: "EUROSTAT", survey: "NAMA_10R.3GDP", dimensions: ["Old"], value: 0 })

        const writer = await datapoints.openDatapointsWriter(URL_A, { replaceSurvey: true })
        await writer.add([mapped(5, { dimensions: ["Lovech", "Euro per inhabitant", "2030"] })])
        await writer.add([mapped(6, { dimensions: ["Lovech", "Euro per inhabitant", "2031"] })]) // not emptied again
        assert.equal(await writer.close(), 2)

        const docs = await db.docs(Orion)
        const demo = d => String(d.survey).toUpperCase() == "DEMO_R_GIND3"
        assert.deepEqual(docs.filter(d => !demo(d)).map(d => d.value).sort(), [1005, 1006])
        assert.equal(docs.filter(demo).length, 1) // other survey untouched
        assert.ok(docs.every(d => d.dupl_hash === undefined)) // inserted, no dedup key
        const years = (await db.docs(Entries, { key: "dimensions" })).map(e => e.value)
        assert.ok(years.includes("2030") && years.includes("2031") && years.includes("Sofia"))
        assert.ok(!years.includes("2020") && !years.includes("2021")) // the old datapoints' values are gone
    })

    test("without replaceSurvey (a { data: { datapoints } } payload): inserted, nothing deleted", async () => {
        await datapoints.upsertDatapoints([mapped(1)], URL_A)
        await datapoints.upsertDatapoints([mapped(1)], URL_A)
        assert.equal((await db.docs(Orion)).length, 2)
    })

    test("Orion records that are not datapoints: inserted after their origin is cleared", async () => {
        const { replaceRecords } = load("utils/sourceRecords.js")
        await replaceRecords([{ city: "Rome" }, { city: "Milan" }], URL_B, { connector: "orion" })
        await replaceRecords([{ city: "Turin" }], URL_B, { connector: "orion" })
        assert.deepEqual((await db.docs(Orion)).map(d => d.city), ["Turin"])
    })
})
