// Real MongoDB 4.4 + PostgreSQL 16 for the tests that touch the databases (entriesStore, sourceRecords, rebuild).
//
// Start them with `npm run test:db` (docker-compose.test.yml, also used by the GitHub workflow) and stop them
// with `npm run test:db:down`. Other servers: MONGO_TEST_URL / PG_TEST_URL.
//
// Every test file gets its own databases (qes_test_<file>), dropped and recreated at start: node --test runs
// the files in parallel. Nothing is written to the databases in config.js.

const env = require("./env")
const path = require("path")

const MONGO_URL = process.env.MONGO_TEST_URL || "mongodb://127.0.0.1:27018"
const PG_URL = process.env.PG_TEST_URL || "postgres://postgres:postgres@127.0.0.1:5433/postgres"
const COLLECTIONS = ["keys", "values", "entries", "sources"]

let mongoose, sharedPg, dbName

function unreachable(what, url, error) {
    return new Error(
        `${what} for the tests is not reachable at ${url.replace(/\/\/[^@/]*@/, "//***@")} (${error.message}).\n` +
        `Start the test databases with "npm run test:db" (Docker), or set MONGO_TEST_URL / PG_TEST_URL.`
    )
}

function pgUrlFor(database) {
    const url = new URL(PG_URL)
    url.pathname = "/" + database
    return url.toString()
}

async function adminPg(sql) {
    const { Client } = require("pg")
    const admin = new Client({ connectionString: PG_URL, connectionTimeoutMillis: 5000 })
    try {
        await admin.connect()
    }
    catch (error) {
        throw unreachable("PostgreSQL", PG_URL, error)
    }
    try {
        await admin.query(sql)
    }
    finally {
        await admin.end()
    }
}

// Connects mongoose and the service's shared pg client (postgresConnector) to fresh databases.
// Call it in before(), before requiring sourceRecords / rebuild / entriesStore.
async function setup(testFile) {
    dbName = "qes_test_" + path.basename(testFile).replace(/\.test\.js$/, "").replace(/\W/g, "_").toLowerCase()

    await adminPg(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`)
    await adminPg(`CREATE DATABASE ${dbName}`)
    env.setBaseConfig({
        postgreConfig: { connectionString: pgUrlFor(dbName) },
        mongo: MONGO_URL
    })

    mongoose = require("mongoose")
    try {
        await mongoose.connect(MONGO_URL, { dbName, serverSelectionTimeoutMS: 5000 })
    }
    catch (error) {
        throw unreachable("MongoDB", MONGO_URL, error)
    }
    await mongoose.connection.dropDatabase()

    // the shared client connects and creates the `sources` table as soon as it is required
    sharedPg = env.load("inputConnectors/postgresConnector.js")
    const started = Date.now()
    while (process.postgreInit !== "done") {
        if (Date.now() - started > 10000)
            throw new Error("postgresConnector did not finish its initialization")
        await new Promise(resolve => setTimeout(resolve, 50))
    }
    await query("SELECT 1 FROM sources LIMIT 1") // fails here if the connection or the table creation failed
}

// Empties the collections (indexes are kept) and the PostgreSQL sources table, restores the config.
async function clean() {
    env.resetConfig()
    for (const name of COLLECTIONS)
        await mongoose.connection.db.collection(name).deleteMany({})
    await query("TRUNCATE sources RESTART IDENTITY")
}

async function teardown() {
    try {
        await mongoose?.connection?.dropDatabase()
    }
    catch { }
    await mongoose?.disconnect().catch(() => { })
    await sharedPg?.end().catch(() => { })
    if (dbName)
        await adminPg(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`).catch(() => { })
}

// Query on the shared client: it runs after the queries the code already queued on it
// (e.g. the INSERT that insertToPostgre fires without awaiting it).
async function query(sql, params) {
    return (await sharedPg.query(sql, params)).rows
}

// Documents of a model's collection without _id
async function docs(Model, filter = {}) {
    return (await Model.collection.find(filter).toArray()).map(({ _id, ...doc }) => doc)
}

module.exports = { setup, clean, teardown, query, docs, MONGO_URL, PG_URL }
