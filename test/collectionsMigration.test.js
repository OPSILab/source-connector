// Move to one collection per connector (utils/collectionsMigration.js), on real MongoDB (npm run test:db).
const { load } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

let migration, Api, Minio, Orion, status

before(async () => {
    await db.setup(__filename)
    migration = load("utils/collectionsMigration.js")
    const collections = load("utils/collections.js")
    Api = collections.collectionModel("api")
    Minio = collections.collectionModel("minio")
    Orion = collections.collectionModel("orion")
    status = require("mongoose").connection.db.collection("status")
})
after(db.teardown)
beforeEach(db.clean)

// the single `sources` collection as it was: API records, MinIO files (tracked in status), maybe datapoints
async function oldSources() {
    await Api.collection.insertMany([
        { source: "https://api/a", sourceId: 1, k: "v" },
        { name: "f.json", record: { bucketName: "public-data" }, json: [{ a: 1 }] },
        { name: "g.csv", record: { s3: { bucket: { name: "pilot" } } }, csv: [{ b: 2 }] },
        { source: "https://eurostat/x.xml", survey: "S", dimensions: ["d"], value: 1 }
    ])
    await status.insertMany([{ refId: "x", source: "minio" }, { refId: "y", source: "other" }])
}

describe("migrateCollections", () => {
    test("dry run: counts only", async () => {
        await oldSources()
        const stats = await migration.migrateCollections({ dryRun: true })
        assert.deepEqual([stats.minioInApi, stats.datapointsInApi, stats.minioStatus], [2, 1, 1])
        assert.deepEqual(stats.collections, { api: "sources", orion: "datapoints", minio: "minio" })
        assert.equal((await db.docs(Api)).length, 4)
        assert.equal((await db.docs(Minio)).length, 0)
    })

    test("MinIO documents moved (same _id), status of MinIO deleted, datapoints kept unless asked, fromUrl index", async () => {
        await oldSources()
        const before = await Api.collection.find({ name: "f.json" }).toArray()
        const stats = await migration.migrateCollections({ batchSize: 1 })
        assert.deepEqual([stats.minioCopied, stats.minioRemoved, stats.statusDeleted, stats.datapointsDeleted], [2, 2, 1, 0])
        assert.deepEqual((await db.docs(Minio)).map(d => d.name).sort(), ["f.json", "g.csv"])
        assert.deepEqual(String((await Minio.collection.find({ name: "f.json" }).toArray())[0]._id), String(before[0]._id))
        assert.equal((await db.docs(Api)).length, 2)
        assert.deepEqual((await status.find({}).toArray()).map(({ _id, ...d }) => d), [{ refId: "y", source: "other" }])
        assert.ok((await Orion.collection.indexes()).some(i => JSON.stringify(i.key) == '{"fromUrl":1}'))

        const again = await migration.migrateCollections({ deleteDatapoints: true })
        assert.deepEqual([again.minioCopied, again.datapointsDeleted], [0, 1])
        assert.deepEqual(await db.docs(Api), [{ source: "https://api/a", sourceId: 1, k: "v" }])
    })

    test("a MinIO document already copied (interrupted run) is not copied twice", async () => {
        await oldSources()
        const [doc] = await Api.collection.find({ name: "f.json" }).toArray()
        await Minio.collection.insertOne(doc)
        const stats = await migration.migrateCollections()
        assert.equal(stats.minioCopied, 1)
        assert.equal((await db.docs(Minio)).length, 2)
    })
})
