// One-off move to one collection per connector (utils/collections.js), from the single `sources` collection where
// apiConnector records and MinIO files were mixed (MinIO ones tracked in the `status` collection):
//
//   1. MinIO documents (record.bucketName / record.s3) found in the API collection are copied to the MinIO collection
//      (same _id: running it again skips them) and removed from the API collection;
//   2. the `status` documents of MinIO are deleted (no longer used);
//   3. datapoints found in the API collection (survey + dimensions: written there by an intermediate version) are
//      counted, and deleted only with deleteDatapoints (the Orion collection keeps the real ones);
//   4. the origin index of the Orion collection (fromUrl) is built: on tens of millions of datapoints it takes a
//      while, better now than at the first Orion notification.
// keys / values / entries are NOT written here: run the rebuild afterwards (POST /api/rebuild?mode=entries), and the
// MinIO sync (it runs at start, or PUT /api/query).
// dryRun: counts only.

const mongoose = require("mongoose")
const logger = require("percocologger")
const { collectionSettings, collectionModel } = require("./collections")
const { DATAPOINT_FILTER } = require("./datapointRules")

const MINIO_FILTER = { $or: [{ "record.bucketName": { $exists: true } }, { "record.s3": { $exists: true } }] }
const DUPLICATE_KEY = 11000

async function copySkippingCopied(target, docs) {
    try {
        await target.insertMany(docs, { ordered: false })
        return docs.length
    }
    catch (error) {
        const writeErrors = [].concat(error.writeErrors || [])
        if (!writeErrors.length || writeErrors.some(e => (e.code ?? e.err?.code) !== DUPLICATE_KEY))
            throw error
        return docs.length - writeErrors.length
    }
}

async function migrateCollections({ dryRun = false, deleteDatapoints = false, batchSize = 1000 } = {}) {
    const api = collectionSettings("api")
    const minio = collectionSettings("minio")
    const orion = collectionSettings("orion")
    const apiColl = collectionModel("api").collection
    const minioColl = collectionModel("minio").collection
    const statusColl = mongoose.connection.db.collection("status")
    const stats = {
        minioInApi: await apiColl.countDocuments(MINIO_FILTER),
        datapointsInApi: await apiColl.countDocuments(DATAPOINT_FILTER),
        minioStatus: await statusColl.countDocuments({ source: "minio" }),
        minioCopied: 0, minioRemoved: 0, statusDeleted: 0, datapointsDeleted: 0, orionIndex: null,
        collections: { api: api.mongo, orion: orion.mongo, minio: minio.mongo }
    }
    if (dryRun)
        return stats

    // 1. MinIO documents: copy in batches, then remove the copied ones
    for (; ;) {
        const batch = await apiColl.find(MINIO_FILTER).limit(batchSize).toArray()
        if (!batch.length)
            break
        stats.minioCopied += await copySkippingCopied(minioColl, batch)
        const { deletedCount } = await apiColl.deleteMany({ _id: { $in: batch.map(d => d._id) } })
        stats.minioRemoved += deletedCount
    }
    // 2. status of MinIO
    stats.statusDeleted = (await statusColl.deleteMany({ source: "minio" })).deletedCount
    // 3. datapoints of the intermediate version
    if (deleteDatapoints && stats.datapointsInApi)
        stats.datapointsDeleted = (await apiColl.deleteMany(DATAPOINT_FILTER)).deletedCount
    // 4. origin index of the Orion collection
    stats.orionIndex = await collectionModel("orion").collection.createIndex({ fromUrl: 1 })
    logger.info("Collections migration finished", stats)
    return stats
}

module.exports = { migrateCollections, MINIO_FILTER }
