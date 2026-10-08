// Format "object" on the Orion refs without the Orion rebuild (utils/formatsMigration.js), on real MongoDB
// (npm run test:db).
const { load } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

let migration, store, Orion, Key, Values, Entries

before(async () => {
    await db.setup(__filename)
    migration = load("utils/formatsMigration.js")
    store = load("utils/entriesStore.js")
    Orion = load("utils/collections.js").collectionModel("orion")
    Key = load("api/models/Key.js")
    Values = load("api/models/Value.js")
    Entries = load("api/models/Entries.js")
})
after(db.teardown)
beforeEach(db.clean)

const DP = "https://eurostat/nama.xml"
const GEO = "https://orion/dataset.geojson"
const PLAIN = "https://orion/records.json"

// as written before the formats: Orion refs (datapoints and other records) and an API ref, without format
async function before_formats() {
    await store.writeAccumulator({ dimensions: { Lovech: ["public-data"] }, survey: { NAMA: ["public-data"] }, city: { Rome: ["public-data"] } }, DP, {}, "orion")
    await store.writeAccumulator({ city: { Rome: ["public-data"] } }, GEO, {}, "orion")
    await store.writeAccumulator({ city: { Rome: ["public-data"] } }, PLAIN, {}, "orion")
    await store.writeAccumulator({ city: { Rome: ["public-data"] } }, "https://api/a", {}, "api")
    // each with its dupl_hash: the Datapoint schema has a unique index on it (upsertRecords), built by mongoose in
    // background - without it the test fails or passes depending on timing
    await Orion.collection.insertMany([
        { fromUrl: DP, survey: "NAMA", dimensions: ["Lovech"], value: 1, dupl_hash: "dp" },
        { fromUrl: GEO, type: "FeatureCollection", features: [{ properties: { city: "Rome" } }], dupl_hash: "geo" },
        { fromUrl: PLAIN, city: "Rome", dupl_hash: "plain" }
    ])
}
const refsOf = async (Model, filter) => (await db.docs(Model, filter))[0].refs.map(r => [r.origin, r.format ?? null]).sort()

describe("migrateOrionFormats", () => {
    test("dry run: counts, and the Orion origins with records that are not plain objects", async () => {
        await before_formats()
        const stats = await migration.migrateOrionFormats({ dryRun: true })
        assert.equal(stats.datapointOrigins, 1) // from the keys survey / dimensions: no datapoint read
        assert.deepEqual(stats.otherOrigins, { [GEO]: "geojson", [PLAIN]: "object" })
        assert.deepEqual(stats.originsLeftOut, { [GEO]: "geojson" })
        assert.deepEqual(stats.documents, { entries: 3, key: 3, value: 3 })
        assert.equal((await db.docs(Entries, { key: "city" }))[0].formats, undefined)
    })

    test("Orion refs \"object\" (datapoints and plain records), the others untouched; `formats` recomputed", async () => {
        await before_formats()
        await migration.migrateOrionFormats()
        assert.deepEqual(await refsOf(Entries, { key: "city" }), [[GEO, null], [PLAIN, "object"], [DP, "object"], ["https://api/a", null]].sort())
        const city = (await db.docs(Entries, { key: "city" }))[0]
        assert.deepEqual(city.formats, ["object"])
        assert.deepEqual((await db.docs(Key, { key: "dimensions" }))[0].formats, ["object"])
        assert.deepEqual((await db.docs(Values, { value: "Lovech" }))[0].refs, [{ origin: DP, visibility: "public-data", connector: "orion", format: "object" }])
        // the same ref written again by the Source-Connector is not added twice
        await store.writeAccumulator({ dimensions: { Lovech: ["public-data"] } }, DP, {}, "orion", "object")
        assert.equal((await db.docs(Entries, { key: "dimensions" }))[0].refs.length, 1)
        // re-run: nothing left
        assert.deepEqual((await migration.migrateOrionFormats({ dryRun: true })).documents, { entries: 0, key: 0, value: 0 })
    })

    test("checkRecords = false: every Orion ref \"object\"", async () => {
        await before_formats()
        const stats = await migration.migrateOrionFormats({ checkRecords: false })
        assert.deepEqual(stats.originsLeftOut, {})
        assert.deepEqual(await refsOf(Entries, { key: "city" }), [[GEO, "object"], [PLAIN, "object"], [DP, "object"], ["https://api/a", null]].sort())
    })
})
