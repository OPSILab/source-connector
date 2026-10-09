// PostgreSQL table of a MinIO bucket (utils/collections.bucketTable): never the table of another connector, so the
// MinIO files never write in (nor delete from) the API / Orion tables.
const { load, config, resetConfig } = require("./helpers/env")
const { test, describe, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

const { bucketTable } = load("utils/collections.js")

beforeEach(resetConfig)

describe("bucketTable", () => {
    test("the bucket name", () => {
        assert.equal(bucketTable("pilot"), "pilot")
    })

    test("the tables of the API and Orion connectors (default or configured) get _table", () => {
        assert.equal(bucketTable("sources"), "sources_table")
        assert.equal(bucketTable("datapoints"), "datapoints_table")
        config.collections.api.postgres = "api_records"
        config.collections.orion.postgres = "eurostat"
        assert.equal(bucketTable("api_records"), "api_records_table")
        assert.equal(bucketTable("eurostat"), "eurostat_table")
        assert.equal(bucketTable("datapoints"), "datapoints") // no longer the Orion table
        assert.equal(bucketTable("default"), "default_table")
        assert.equal(bucketTable("status"), "status_table")
        assert.equal(bucketTable("users"), "users_table")
        assert.equal(bucketTable("credentials"), "credentials_table")
    })

    test("invalid names are refused", () => {
        assert.throws(() => bucketTable("1abc"), /Invalid/)
        assert.throws(() => bucketTable(undefined), /Invalid/)
    })
})
