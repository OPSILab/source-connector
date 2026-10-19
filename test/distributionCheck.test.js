// When an Orion DistributionDCAT-AP entity is mapped again (utils/distributionCheck.js), and the daily check reading
// every Orion entity (utils/common.js).
const { stub, load, config, resetConfig } = require("./helpers/env")
const { test, describe, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")

// the entities collection: { entityId, downloadURL, byteSize }; findOne(...) awaitable or .lean()
let stored = []
const query = value => ({ lean: async () => value, then: (resolve, reject) => Promise.resolve(value).then(resolve, reject) })
stub("api/models/Entity.js", {
    findOne: filter => query(stored.find(doc =>
        filter.entityId ? doc.entityId == filter.entityId : (filter.$or || []).some(f => Object.values(f)[0] == doc.downloadURL.value)) || null)
})
const check = load("utils/distributionCheck.js")
const common = load("utils/common.js")

const P = value => ({ type: "Property", value })
const URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/rd_e_gerdreg?format=sdmx_2.1_structured&compressed=true"
const A = "urn:ngsi-ld:DistributionDCAT-AP:id:39cda7cf"
const B = "urn:ngsi-ld:DistributionDCAT-AP:id:c03a7cc1"
const entity = (id, byteSize, sys = {}) => ({ id, type: "DistributionDCAT-AP", downloadURL: P(URL), byteSize: P(byteSize), checksum: P(""), format: P("XML"), ...sys })
const storedDoc = (id, byteSize) => ({ _id: "x", ...entity(id, byteSize), entityId: id })

describe("changedFields: plain values, however they are wrapped", () => {
    const orion = entity(A, "1634109")
    const wrapped = common.fastAndPartialOrionizeEntity({ ...orion, entityId: A })

    test("same values: nothing changed, in every combination of wrappings", () => {
        for (const [e, s] of [[orion, storedDoc(A, "1634109")], [orion, wrapped], [wrapped, wrapped], [wrapped, storedDoc(A, "1634109")]])
            assert.deepEqual(check.changedFields(e, s), [])
    })

    test("a different byteSize is a change, in every combination (the old check missed it with one side wrapped)", () => {
        const changed = entity(A, "1659321")
        for (const [e, s] of [[changed, wrapped], [common.fastAndPartialOrionizeEntity(changed), storedDoc(A, "1634109")], [changed, storedDoc(A, "1634109")]])
            assert.deepEqual(check.changedFields(e, s), ["byteSize"])
    })

    test("a number and the same number as text are the same", () => {
        assert.deepEqual(check.changedFields(entity(A, 1634109), storedDoc(A, "1634109")), [])
    })

    test("modifiedDate (@value): equal is not a change (the old check always saw one), different is", () => {
        const md = value => P({ "@type": "DateTime", "@value": value })
        const e = { ...entity(A, "1"), modifiedDate: md("2026-01-01") }
        const s = { ...storedDoc(A, "1"), modifiedDate: md("2026-01-01") }
        assert.deepEqual(check.changedFields(common.fastAndPartialOrionizeEntity(e), common.fastAndPartialOrionizeEntity(s)), [])
        assert.deepEqual(check.changedFields({ ...e, modifiedDate: md("2026-02-01") }, s), ["modifiedDate"])
        assert.deepEqual(check.changedFields(e, storedDoc(A, "1")), ["modifiedDate"]) // not stored before
    })

    test("a value that appears is a change, one that disappears is not; nothing stored: everything", () => {
        assert.deepEqual(check.changedFields(entity(A, "5"), storedDoc(A, "")), ["byteSize"])
        assert.deepEqual(check.changedFields(entity(A, ""), storedDoc(A, "5")), [])
        assert.deepEqual(check.changedFields(entity(A, "5"), null), check.CHECKED_FIELDS)
    })

    test("a mongoose document is read through toObject", () => {
        const doc = { toObject: () => storedDoc(A, "1634109"), byteSize: "not this" }
        assert.deepEqual(check.changedFields(entity(A, "1634109"), doc), [])
    })
})

describe("daily check (verifyLostSubscriptionOrion)", () => {
    let orion, posted
    beforeEach(() => {
        resetConfig()
        Object.assign(config.orion, { orionBaseUrl: "http://orion", subscribeType: "DistributionDCAT-AP", attrWithUrl: "downloadURL", useNgsiBroker: false })
        posted = []
        axios.get = async url => ({ data: orion, headers: { "ngsild-results-count": String(orion.length) } })
        axios.post = async (url, body) => { posted.push(...body.data.map(e => e.id)) }
    })
    test("an unchanged entity is not sent, a changed one is", async () => {
        orion = [entity(A, "1634109"), entity(B, "5")]
        stored = [storedDoc(A, "1634109"), storedDoc(B, "4")]
        await common.verifyLostSubscriptionOrion()
        assert.deepEqual(posted, [B])
    })


    test("every entity is read, past the first 1000 (count header): the 500 after them, not stored, are sent", async () => {
        const page = (n, from) => Array.from({ length: n }, (_, i) => entity("urn:" + (from + i), "1", {}))
        const all = page(1500, 0).map((e, i) => ({ ...e, downloadURL: P(URL + i) }))
        stored = all.slice(0, 1000).map(e => ({ ...e, entityId: e.id }))
        axios.get = async url => {
            const offset = Number(new URLSearchParams(url.split("?")[1]).get("offset") || 0)
            return { data: all.slice(offset, offset + 1000), headers: { "ngsild-results-count": "1500" } }
        }
        await common.verifyLostSubscriptionOrion()
        assert.deepEqual(posted, all.slice(1000).map(e => e.id))
        stored = []
    })
})
