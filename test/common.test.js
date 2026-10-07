const { stub, load } = require("./helpers/env")
const { test, describe } = require("node:test")
const assert = require("node:assert/strict")

stub("api/models/Entity.js", { findOne: async () => null })
const common = load("utils/common.js")

async function entriesOf(obj, type, name) {
    const entries = {}
    await common.getEntries(obj, type, name, entries)
    return entries
}

describe("getEntries", () => {
    test("json object: one entry per key, visibility public-data without a name", async () => {
        const entries = await entriesOf([{ city: "Rome", population: 2800000, tags: ["a", "b"] }], "json")
        assert.deepEqual(entries, {
            city: { Rome: ["public-data"] },
            population: { "2800000": ["public-data"] },
            tags: { '["a","b"]': ["public-data"] }
        })
    })

    test("jsonArray ({ json: [...] }): values of all items, no duplicated visibility", async () => {
        const entries = await entriesOf([{ json: [{ k: "x" }, { k: "y" }, { k: "x" }] }], "jsonArray", "public-data/file.json")
        assert.deepEqual(entries, { k: { x: ["public-data"], y: ["public-data"] } })
    })

    test("the type is corrected from the payload shape", async () => {
        // declared json, actually a jsonArray
        const entries = await entriesOf([{ json: [{ k: "v" }] }], "json")
        assert.deepEqual(entries, { k: { v: ["public-data"] } })
    })

    test("csv ({ csv: [...] })", async () => {
        const entries = await entriesOf([{ csv: [{ a: "1", b: "2" }] }], "json")
        assert.deepEqual(entries, { a: { "1": ["public-data"] }, b: { "2": ["public-data"] } })
    })

    test("geojson: the properties of the features are indexed", async () => {
        const geojson = { type: "FeatureCollection", features: [{ type: "Feature", properties: { name: "A" } }, { type: "Feature", properties: { name: "B" } }] }
        const entries = await entriesOf([geojson], "json")
        assert.deepEqual(entries, { name: { A: ["public-data"], B: ["public-data"] } })
    })

    test("visibility from the object name: private (email), shared, public", async () => {
        assert.deepEqual(await entriesOf([{ k: "v" }], "json", "user@demetrix.it/data model mapper/f.json"), { k: { v: ["user@demetrix.it"] } })
        assert.deepEqual(await entriesOf([{ k: "v" }], "json", "PILOT SHARED Data/f.json"), { k: { v: ["PILOT SHARED Data"] } })
        assert.deepEqual(await entriesOf([{ k: "v" }], "json", "folder/f.json"), { k: { v: ["public-data"] } })
    })

    test("accumulates over calls and merges visibilities of the same key/value", async () => {
        const entries = {}
        await common.getEntries([{ k: "v" }], "json", "a@b.it/x", entries)
        await common.getEntries([{ k: "v" }], "json", "a@b.it/x", entries)
        await common.getEntries([{ k: "v", other: 1 }], "json", undefined, entries)
        assert.deepEqual(entries, { k: { v: ["a@b.it", "public-data"] }, other: { "1": ["public-data"] } })
    })
})

describe("setType", () => {
    test("csv / jsonArray / json / raw", async () => {
        assert.equal(await common.setType("csv", "a,b"), "csv")
        assert.equal(await common.setType("json", [1, 2]), "jsonArray")
        assert.equal(await common.setType("json", { a: 1 }), "json")
        assert.equal(await common.setType("xml", "<a/>"), "raw")
    })
})

describe("convertCSVtoJSON", () => {
    test("comma separated, quotes and surrounding spaces removed", () => {
        const json = JSON.parse(common.convertCSVtoJSON('"name", "age"\r\n"Ann", 30\r\nBob ,41'))
        assert.deepEqual(json, [{ name: "Ann", age: "30" }, { name: "Bob", age: "41" }])
    })

    test("semicolon separated", () => {
        const json = JSON.parse(common.convertCSVtoJSON("a;b;c\r\n1;2;3"))
        assert.deepEqual(json, [{ a: "1", b: "2", c: "3" }])
    })

    test("missing cells become undefined (dropped by JSON)", () => {
        const json = JSON.parse(common.convertCSVtoJSON("a,b\r\n1"))
        assert.deepEqual(json, [{ a: "1" }])
    })
})

describe("checkConfig", () => {
    test("fills missing keys (also nested) from the template, keeps the existing ones", () => {
        const result = common.checkConfig(
            { port: 1, nested: { a: "mine" } },
            { port: 2, extra: true, nested: { a: "template", b: "template" } }
        )
        assert.deepEqual(result, { port: 1, extra: true, nested: { a: "mine", b: "template" } })
    })
})

describe("small helpers", () => {
    test("deleteSpaces trims only spaces", () => {
        assert.equal(common.deleteSpaces("  a b  "), "a b")
        assert.equal(common.deleteSpaces(undefined), undefined)
    })

    test("urlEncode drops dashes", () => {
        assert.equal(common.urlEncode("public-data"), "publicdata")
    })

    test("parseJwt decodes the payload", () => {
        const payload = Buffer.from(JSON.stringify({ email: "a@b.it" })).toString("base64")
        assert.deepEqual(common.parseJwt(`h.${payload}.s`), { email: "a@b.it" })
    })

    test("cleaned removes quotes and newlines", () => {
        assert.equal(common.cleaned("it's\r\nok"), "itsok")
        assert.equal(common.cleaned({ a: 1 }), '{"a":1}')
    })
})
