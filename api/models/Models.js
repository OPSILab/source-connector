// One model per collection, on the collection named in config.collections (see utils/collections.js). Created at
// the first use of each name, so a different name in the config gets its own model.
const mongoose = require("mongoose")
const { datapointSchema } = require("./Datapoint")

function modelOn(kind, schema, collection) {
    const name = kind + ":" + collection
    return mongoose.models[name] || mongoose.model(name, schema, collection)
}

const schemaless = () => new mongoose.Schema({}, { strict: false, versionKey: false })
const recordSchema = schemaless()
const minioSchema = schemaless()

module.exports = {
    // apiConnector records (and, for the old code, the Source model)
    recordModel: collection => modelOn("record", recordSchema, collection),
    // Orion datasets: datapoints (legacy schema, upsertMany by dupl_hash) and records
    datapointModel: collection => modelOn("datapoint", datapointSchema, collection),
    // MinIO files
    minioModel: collection => modelOn("minio", minioSchema, collection)
}
