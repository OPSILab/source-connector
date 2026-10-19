// When a synchronization of an Orion dataset starts, on the real situation of October 2026
// (test/fixtures/orion-eurostat):
//  - orion-sdmx-distributions.json: the 44 Eurostat SDMX distributions in Orion. 33 datasets: 11 of them were harvested
//    twice (two ids, same downloadURL, same byteSize and checksum), no modifiedDate;
//  - stored-entities.json: the 33 entities stored in MongoDB (one per downloadURL), after the cleanup of the
//    rd_e_gerdreg / road_go_na_rl3g / road_go_na_ru3g duplicates.
// The flow is the real one: the daily check (common.verifyLostSubscriptionOrion) reads Orion and posts to
// /orion/subscribe, the notification runs executeRequest (api/services/service.js). The synchronization starts when
// executeRequest asks the mapper for the map of the dataset: that request is recorded in `synchronized` (and fails,
// so nothing is written). The database, Orion, Keycloak and the mapper are replaced by stubs.
const { stub, load, config, resetConfig } = require("./helpers/env")
const { test, describe, before, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")

const ORION = require("./fixtures/orion-eurostat/orion-sdmx-distributions.json")
const STORED = require("./fixtures/orion-eurostat/stored-entities.json")
const MAP_ID = "6914a252ddb96948ee67b2e1"

// The entities collection: findOne by entityId / by "downloadURL.value" (dotted paths, exact values)
let db = []
const written = []
const valueAt = (doc, path) => path.split(".").reduce((value, key) => value?.[key], doc)
const matches = (doc, filter) => Object.entries(filter).every(([path, value]) => valueAt(doc, path) === value)
stub("api/models/Entity.js", {
    findOne: async filter => structuredClone(db.find(doc => matches(doc, filter)) || null),
    findOneAndUpdate: async (filter, update) => { written.push(update) },
    insertMany: async docs => { written.push(...docs) }
})
stub("utils/keycloak.js", { updateJWT: async () => "test-token" })
stub("inputConnectors/minioConnector.js", {})
stub("inputConnectors/orionConnector.js", {})
stub("api/models/Dimensions.js", {})
stub("utils/sourceRecords.js", { replaceRecords: async () => { throw new Error("replaceRecords not expected") } })
stub("utils/datapoints.js", { upsertDatapoints: async () => { }, openDatapointsWriter: async () => { } })

config.orion.attrWithUrl = "downloadURL" // read by service.js when it is loaded
const service = load("api/services/service.js")
const common = load("utils/common.js")

let orion, posted, synchronized, results

const urlOf = entity => entity.downloadURL.value
const code = url => url.split("/data/")[1].split("?")[0]
const orionEntity = datasetCode => orion.filter(entity => code(urlOf(entity)) == datasetCode)
const storedIds = () => new Set(STORED.map(doc => doc.entityId))

before(() => new Promise(resolve => setImmediate(resolve))) // the Keycloak token of service.js

beforeEach(() => {
    resetConfig()
    Object.assign(config.orion, { orionBaseUrl: "http://orion", subscribeType: "DistributionDCAT-AP", attrWithUrl: "downloadURL", useNgsiBroker: false })
    db = structuredClone(STORED)
    written.length = 0
    orion = structuredClone(ORION)
    posted = []
    synchronized = []
    results = []
    axios.get = async (url, options) => {
        if (url.startsWith("http://orion/ngsi-ld/v1/entities")) {
            const offset = Number(new URL(url).searchParams.get("offset") || 0)
            return { data: structuredClone(orion.slice(offset, offset + 1000)), headers: { "ngsild-results-count": String(orion.length) } }
        }
        if (url == config.getMapEndpoint) {
            synchronized.push(options.params.description)
            throw Object.assign(new Error("no map"), { response: { status: 404 } })
        }
        throw new Error("unexpected GET " + url)
    }
    axios.post = async (url, body) => {
        if (url.endsWith("/api/orion/subscribe/" + MAP_ID)) {
            posted.push(...body.data.map(entity => entity.id))
            results.push(await notify(body))
            return { data: "OK" }
        }
        if (url == config.parseEndpoint || url == config.mapEndpoint)
            throw new Error("the mapper is not available in the tests")
        throw new Error("unexpected POST " + url)
    }
})

// POST /orion/subscribe/:mapID
const notify = body => service.notifyPath({ body, params: { mapID: MAP_ID }, query: {} }, {})

async function dailyCheck() {
    await common.verifyLostSubscriptionOrion()
    assert.ok(results.every(result => result === "OK"), "every notification ended normally: " + results.filter(r => r !== "OK"))
}

describe("the fixtures", () => {
    test("44 distributions in Orion, 33 urls, all stored once; 11 urls twice with the same byteSize and checksum", () => {
        assert.equal(ORION.length, 44)
        const byUrl = new Map()
        for (const entity of ORION)
            byUrl.set(urlOf(entity), [...(byUrl.get(urlOf(entity)) || []), entity])
        assert.equal(byUrl.size, 33)
        assert.equal(STORED.length, 33)
        assert.deepEqual(new Set(STORED.map(urlOf)), new Set(byUrl.keys()))
        const doubled = [...byUrl.values()].filter(copies => copies.length > 1)
        assert.equal(doubled.length, 11)
        for (const [first, second] of doubled) {
            assert.equal(first.byteSize.value, second.byteSize.value)
            assert.equal(first.checksum.value, second.checksum.value)
        }
        assert.ok(ORION.every(entity => !entity.modifiedDate))
        // every stored entity is one of the Orion entities, with its byteSize and checksum
        for (const doc of STORED) {
            const entity = ORION.find(entity => entity.id == doc.entityId)
            assert.ok(entity, doc.entityId)
            assert.equal(doc.byteSize.value, entity.byteSize.value)
            assert.equal(doc.checksum.value, entity.checksum.value)
        }
    })
})

describe("nothing changed in Orion: no synchronization starts", () => {
    test("daily check: the 11 copies not stored by id are posted, found by url and skipped; none synchronized", async () => {
        await dailyCheck()
        assert.deepEqual(synchronized, [])
        assert.deepEqual(written, [])
        const notStoredById = ORION.filter(entity => !storedIds().has(entity.id)).map(entity => entity.id)
        assert.equal(notStoredById.length, 11)
        assert.deepEqual(posted.sort(), notStoredById.sort())
    })

    test("the 44 notifications as Orion sends them: all skipped", async () => {
        for (const entity of orion)
            assert.equal(await notify({ type: "Notification", subscriptionId: "orion", data: [entity] }), "OK")
        assert.deepEqual(synchronized, [])
        assert.deepEqual(written, [])
    })
})

describe("byteSize or checksum changed in Orion: the synchronization of that dataset starts", () => {
    test("byteSize changed (rd_e_gerdreg): that dataset only", async () => {
        const [entity] = orionEntity("rd_e_gerdreg")
        assert.equal(entity.byteSize.value, "1659321")
        entity.byteSize.value = "1660000"
        await dailyCheck()
        assert.deepEqual(synchronized, [urlOf(entity)])
    })

    test("checksum changed (demo_r_gind3): that dataset only", async () => {
        const [entity] = orionEntity("demo_r_gind3")
        assert.equal(entity.checksum.value, "")
        entity.checksum.value = "sha256:1f2d3c4b5a69788796a5b4c3d2e1f0aa"
        await dailyCheck()
        assert.deepEqual(synchronized, [urlOf(entity)])
    })
})

describe("other fields changed in Orion: no synchronization starts", () => {
    test("description, title, accessUrl, mediaType, license, rights, status and language changed on all 44", async () => {
        for (const entity of orion) {
            entity.description.value = "A new description"
            entity.title.value = { en: "A new title", it: "Un nuovo titolo" }
            entity.accessUrl.value = urlOf(entity) + "&changed=true"
            entity.mediaType.value = "application/vnd.sdmx.structurespecificdata+xml"
            entity.license.value = "http://publications.europa.eu/resource/authority/licence/CC_BY_4_0"
            entity.rights.value = "http://publications.europa.eu/resource/authority/access-right/RESTRICTED"
            entity.status.value = "http://publications.europa.eu/resource/authority/distribution-status/COMPLETED"
            entity.language.value = ["http://publications.europa.eu/resource/authority/language/ENG"]
        }
        await dailyCheck()
        assert.deepEqual(synchronized, [])
        assert.deepEqual(written, [])
    })
})
