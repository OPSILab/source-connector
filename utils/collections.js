// One MongoDB collection (and one PostgreSQL table) per source connector, from config.collections:
//
//   api    apiConnector records                     mongo "sources",    postgres "sources"
//   orion  Orion datasets (datapoints and records)  mongo "datapoints", postgres "datapoints"
//   minio  MinIO files                              mongo "minio",      postgres: one table per bucket
//
// toMongo / toPostgres choose where each connector writes. queryOptions.advancedSearch === false turns every MongoDB
// write off and queryOptions.SQLQuery === false every PostgreSQL write (they were the global switches before).
// Keys / values / entries are written only for the data stored in MongoDB (they are the suggestions of its search).

const config = require("../config")

const CONNECTORS = ["api", "orion", "minio"]

const DEFAULTS = {
    api: { mongo: "sources", toMongo: true, postgres: "sources", toPostgres: true },
    orion: { mongo: "datapoints", toMongo: true, postgres: "datapoints", toPostgres: false },
    minio: { mongo: "minio", toMongo: true, toPostgres: true }
}

// The field holding the origin (url) of a record: replace / rebuild / remove work by origin.
// MinIO documents have no such field: their origin is the file (see minioConnector.minioOrigin).
const ORIGIN_FIELD = { api: "source", orion: "fromUrl" }

const SQL_NAME = /^[a-zA-Z_][a-zA-Z0-9_]*$/

function collectionSettings(connector) {
    if (!CONNECTORS.includes(connector))
        throw new Error(`Unknown connector "${connector}" (${CONNECTORS.join(" | ")})`)
    const s = { ...DEFAULTS[connector], ...(config.collections?.[connector] || {}) }
    return {
        connector,
        mongo: s.mongo,
        postgres: s.postgres,
        toMongo: s.toMongo !== false && config.queryOptions?.advancedSearch !== false,
        toPostgres: s.toPostgres === true && config.queryOptions?.SQLQuery !== false,
        originField: ORIGIN_FIELD[connector]
    }
}

// Checked at startup: two connectors in the same collection / table would delete each other's data.
function validateCollections() {
    const errors = []
    const seen = { mongo: {}, postgres: {} }
    for (const connector of CONNECTORS) {
        const s = collectionSettings(connector)
        if (typeof s.mongo !== "string" || !s.mongo || s.mongo.includes("$") || s.mongo.startsWith("system."))
            errors.push(`collections.${connector}.mongo: invalid collection name`)
        else if (seen.mongo[s.mongo])
            errors.push(`collections.${connector}.mongo: "${s.mongo}" is already the collection of ${seen.mongo[s.mongo]}`)
        else
            seen.mongo[s.mongo] = connector
        if (connector == "minio")
            continue // one table per bucket
        if (typeof s.postgres !== "string" || !SQL_NAME.test(s.postgres))
            errors.push(`collections.${connector}.postgres: invalid table name`)
        else if (seen.postgres[s.postgres])
            errors.push(`collections.${connector}.postgres: "${s.postgres}" is already the table of ${seen.postgres[s.postgres]}`)
        else
            seen.postgres[s.postgres] = connector
    }
    if (errors.length)
        throw new Error("Invalid config.collections:\n" + errors.join("\n"))
}

// Mongoose model of a connector's collection (required lazily: the models require the config through this module)
function collectionModel(connector) {
    const models = require("../api/models/Models")
    const { mongo } = collectionSettings(connector)
    return { api: models.recordModel, orion: models.datapointModel, minio: models.minioModel }[connector](mongo)
}

// PostgreSQL table of a MinIO bucket (one per bucket): its name, unless reserved - the tables of the other connectors
// (collections.api.postgres, collections.orion.postgres), the ones of the users / credentials and a few more -, then
// "<name>_table". Without this a bucket named like the API / Orion table would write in it, and deleting one of its
// files would delete the rows with the same name there.
function bucketTable(name) {
    if (typeof name !== "string" || !SQL_NAME.test(name))
        throw new Error("Invalid table name")
    const reserved = new Set(["default", "status", "sources", "users", "credentials", ...["api", "orion"].map(c => collectionSettings(c).postgres)])
    return reserved.has(name) ? name + "_table" : name
}

module.exports = { CONNECTORS, DEFAULTS, ORIGIN_FIELD, collectionSettings, validateCollections, collectionModel, bucketTable }
