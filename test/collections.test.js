// config.collections (utils/collections.js): no database needed.
const { load, config, resetConfig } = require("./helpers/env")
const { test, describe, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

const collections = load("utils/collections.js")

beforeEach(resetConfig)

describe("collectionSettings", () => {
    test("defaults: api in sources, orion in datapoints (no PostgreSQL), minio in minio", () => {
        delete config.collections
        assert.deepEqual(collections.collectionSettings("api"), { connector: "api", mongo: "sources", postgres: "sources", toMongo: true, toPostgres: true, originField: "source" })
        assert.deepEqual(collections.collectionSettings("orion"), { connector: "orion", mongo: "datapoints", postgres: "datapoints", toMongo: true, toPostgres: false, originField: "fromUrl" })
        assert.deepEqual(collections.collectionSettings("minio"), { connector: "minio", mongo: "minio", postgres: undefined, toMongo: true, toPostgres: true, originField: undefined })
    })

    test("config overrides, field by field", () => {
        config.collections = { orion: { mongo: "orion_data", toPostgres: true } }
        assert.deepEqual(collections.collectionSettings("orion"), { connector: "orion", mongo: "orion_data", postgres: "datapoints", toMongo: true, toPostgres: true, originField: "fromUrl" })
    })

    test("queryOptions.advancedSearch / SQLQuery = false switch every connector off", () => {
        config.queryOptions.advancedSearch = false
        config.queryOptions.SQLQuery = false
        for (const c of collections.CONNECTORS)
            assert.deepEqual([collections.collectionSettings(c).toMongo, collections.collectionSettings(c).toPostgres], [false, false])
    })

    test("unknown connector", () => {
        assert.throws(() => collections.collectionSettings("ftp"), /Unknown connector/)
    })
})

describe("validateCollections", () => {
    test("the template is valid", () => {
        collections.validateCollections()
    })

    test("two connectors on the same collection or table, invalid names", () => {
        config.collections.minio.mongo = "sources"
        assert.throws(() => collections.validateCollections(), /minio\.mongo: "sources" is already the collection of api/)
        resetConfig()
        config.collections.orion.postgres = "sources"
        assert.throws(() => collections.validateCollections(), /orion\.postgres: "sources" is already the table of api/)
        resetConfig()
        config.collections.api.postgres = "drop table"
        config.collections.orion.mongo = "a$b"
        assert.throws(() => collections.validateCollections(), error => /api\.postgres: invalid/.test(error.message) && /orion\.mongo: invalid/.test(error.message))
    })
})
