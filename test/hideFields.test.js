// hideFields (utils/hideFields.js): the values at those paths of an API's answers replaced by pseudonyms (HMAC with
// hideFieldsSecret) before anything is stored. Unit tests, then a poll on real MongoDB + PostgreSQL (helpers/db.js).
const { load, config } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")
const { loadApiConnector } = require("./helpers/apiConnector")
const { hideFields, hiddenPaths, pseudonym, PSEUDONYM } = require("../utils/hideFields")

const PATH = "entry.resource.participant.actor.display"
const fhir = () => ({
    resourceType: "Bundle", type: "searchset", total: 2,
    entry: [
        { resource: { id: "a1", participant: [{ actor: { display: "Mario Rossi", reference: "Patient/1" } }, { actor: { display: "Dr. Bianchi" } }] } },
        { resource: { id: "a2", participant: [{ actor: { display: "Mario Rossi" } }, { actor: { reference: "Location/9" } }] } }
    ]
})
const displays = bundle => bundle.entry.flatMap(e => e.resource.participant.map(p => p.actor.display))

describe("hideFields", () => {
    test("through the arrays: every display of every participant of every entry; the rest as it was", () => {
        const data = hideFields(fhir(), { name: "F", hideFields: [PATH] }, "key")
        const [rossi, bianchi, rossiAgain, missing] = displays(data)
        assert.match(rossi, PSEUDONYM)
        assert.notEqual(rossi, bianchi)
        assert.equal(rossi, rossiAgain) // the same person: the same pseudonym (linked)
        assert.equal(missing, undefined) // no display: nothing added
        assert.equal(data.entry[0].resource.participant[0].actor.reference, "Patient/1")
        assert.equal(data.total, 2)
    })

    test("another key: other pseudonyms; already hidden: not hashed again", () => {
        assert.notEqual(pseudonym("Mario Rossi", "key"), pseudonym("Mario Rossi", "other"))
        const once = hideFields(fhir(), { name: "F", hideFields: [PATH] }, "key")
        assert.deepEqual(displays(hideFields(structuredClone(once), { name: "F", hideFields: [PATH] }, "key")), displays(once))
    })

    test("an index, a list answer, values that are not text, arrays of values", () => {
        const data = hideFields(fhir(), { name: "F", hideFields: ["entry.0.resource.participant.actor.display"] }, "key")
        assert.deepEqual(displays(data).map(d => d && PSEUDONYM.test(d)), [true, true, false, undefined])
        const list = hideFields([{ who: "A", n: 1 }, { who: "B" }], { name: "L", hideFields: ["who", "n", "names"] }, "key")
        assert.ok(list.every(item => PSEUDONYM.test(item.who)) && PSEUDONYM.test(list[0].n))
        const names = hideFields({ names: ["A", null, "B"] }, { name: "N", hideFields: ["names"] }, "key").names
        assert.ok(PSEUDONYM.test(names[0]) && names[1] === null && PSEUDONYM.test(names[2]))
    })

    test("no hideFields: as it is; no secret: an error; a bad path: an error", () => {
        assert.deepEqual(hideFields(fhir(), { name: "F" }, ""), fhir())
        assert.throws(() => hideFields(fhir(), { name: "F", hideFields: [PATH] }, ""), error => error.code == "NO_SECRET")
        assert.throws(() => hiddenPaths({ name: "F", hideFields: ["entry..display"] }), /dotted paths/)
        assert.deepEqual(hiddenPaths({ name: "F", hideFields: PATH }), [PATH])
    })
})

describe("a poll of an API with hideFields", () => {
    let pollAPI, Source, Entries, realGet
    before(async () => {
        await db.setup(__filename)
        Source = load("utils/collections.js").collectionModel("api")
        Entries = load("api/models/Entries.js")
        await load("utils/pollStatus.js").ensurePgStatusTable()
        pollAPI = await loadApiConnector()
        realGet = axios.get
    })
    after(db.teardown)
    beforeEach(async () => {
        await db.clean()
        config.hideFieldsSecret = "a long random key"
    })
    afterEach(() => { axios.get = realGet })

    const api = { name: "Appointments", url: "https://fhir.example/Appointment", fhir: true, upsertRecords: true, hideFields: [PATH] }
    const everything = async () => JSON.stringify([
        await Source.find({}).lean(),
        await db.query("SELECT data FROM sources"),
        await Entries.collection.find({}).toArray()
    ])

    test("stored with the pseudonyms: MongoDB, PostgreSQL, keys / values / entries; never the names", async () => {
        axios.get = async () => ({ data: fhir() })
        config.apiConnectorConfig.apiUrls = [api]
        await pollAPI()
        const [stored] = await Source.find({}).lean()
        assert.equal(displays(stored)[0], pseudonym("Mario Rossi", config.hideFieldsSecret))
        const all = await everything()
        assert.ok(!all.includes("Mario Rossi") && !all.includes("Bianchi"), "a name stored")
        assert.ok(all.includes(pseudonym("Mario Rossi", config.hideFieldsSecret)))
    })

    test("no hideFieldsSecret, or an answer that is not JSON: nothing stored", async () => {
        config.apiConnectorConfig.apiUrls = [api]
        config.hideFieldsSecret = ""
        axios.get = async () => ({ data: fhir() })
        await pollAPI()
        config.hideFieldsSecret = "a long random key"
        axios.get = async () => ({ data: "<Bundle><display>Mario Rossi</display></Bundle>" })
        await pollAPI()
        assert.equal((await Source.find({}).lean()).length, 0)
        assert.ok(!(await everything()).includes("Mario Rossi"))
    })
})
