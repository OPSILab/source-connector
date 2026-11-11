// Non incremental API polls (apiConnector, storeSnapshot): every record stored gets datePolled; the answer is compared
// with the records stored for its url by order (a continuation: only the new tail, flagged likelyIncremental), else
// as configured in unalignedAnswer ("upsert": replaced; "append": appended whole, flagged unverifiedAppend), else by
// count. upsertRecords: every answer replaces the records, as before.
// The records and the keys / values / entries are replaced by in-memory stubs: no database needed.
const { stub, load, config, resetConfig } = require("./helpers/env")
const { test, describe, before, beforeEach, afterEach } = require("node:test")
const assert = require("node:assert/strict")
const axios = require("axios")

let records = [], nextId = 1
const matches = (doc, filter) => Object.entries(filter).every(([key, value]) => doc[key] === value)
const query = docs => ({
    sort: spec => query([...docs].sort((a, b) => Object.entries(spec).reduce((r, [k, dir]) => r || (a[k] < b[k] ? -dir : a[k] > b[k] ? dir : 0), 0))),
    lean: async () => structuredClone(docs)
})
const model = {
    find: filter => query(records.filter(doc => matches(doc, filter))),
    findOne: filter => ({ sort: () => ({ lean: async () => structuredClone(records.find(doc => matches(doc, filter)) || null) }) })
}
stub("utils/collections.js", {
    collectionSettings: () => ({ toMongo: true, toPostgres: false, originField: "source", mongo: "sources" }),
    collectionModel: () => model
})
stub("utils/entriesStore.js", { createCollector: () => ({ add: async () => { }, flush: async () => { } }) })
stub("utils/sourceRecords.js", {
    makeItem: (item, origin) => ({ ...item, source: origin }),
    storeRecords: async docs => {
        const ids = docs.map(doc => { const _id = nextId++; records.push({ _id, ...structuredClone(doc) }); return _id })
        return { mongo: ids, postgres: [] }
    },
    clearOrigin: async origin => { records = records.filter(doc => doc.source !== origin) },
    replaceRecords: async () => { throw new Error("replaceRecords not expected") },
    pgQuery: async () => ({ rows: [] }), waitForPostgreInit: async () => { }, ensurePgTable: async () => { }
})
stub("utils/pollStatus.js", { markPending: async () => { }, completePoll: async () => { }, rollbackPending: async () => ({ mongo: 0, postgres: 0 }) })
stub("utils/incrementalState.js", { get: async () => null, windowDone: async () => { }, addMissed: async () => { }, removeMissed: async () => { } })

const URL_A = "https://api.example/snapshot"
let pollAPI, answer
const realGet = axios.get

before(async () => {
    config.apiConnectorConfig.apiUrls = []
    const realSetInterval = global.setInterval
    global.setInterval = (...args) => realSetInterval(...args).unref()
    try {
        pollAPI = load("inputConnectors/apiConnector.js").pollAPI
    }
    finally {
        global.setInterval = realSetInterval
    }
    const locks = load("utils/jobLocks.js")
    while (locks.polling)
        await new Promise(resolve => setTimeout(resolve, 10))
})

beforeEach(() => {
    resetConfig()
    records = []
    axios.get = async (url, options) => {
        const offset = options?.params?.offset
        if (offset === undefined)
            return { data: structuredClone(answer) }
        return { data: structuredClone(answer.slice(offset, offset + 2)) }
    }
    config.apiConnectorConfig.apiUrls = [{ name: "Snapshot", url: URL_A }]
})
afterEach(() => { axios.get = realGet })

const rec = v => ({ v })
const thirty = [...Array.from({ length: 27 }, (_, i) => rec("r" + i)), rec("X"), rec("X"), rec("X")]
const values = () => records.map(doc => doc.v)
const poll = async data => { answer = data; await pollAPI() }

describe("non incremental poll", () => {
    test("first poll: every record, duplicates included, with datePolled", async () => {
        await poll(thirty)
        assert.equal(records.length, 30)
        assert.ok(records.every(doc => doc.datePolled instanceof Date && doc.likelyIncremental === undefined))
    })

    test("the same answer again: nothing", async () => {
        await poll(thirty)
        await poll(thirty)
        assert.equal(records.length, 30)
    })

    test("the window slid, the X that went out replaced by a new X: that X, flagged likelyIncremental", async () => {
        const first = [rec("X"), ...thirty.slice(0, 27), rec("X"), rec("X")]
        await poll(first)
        await poll([...first.slice(1), rec("X")])
        assert.equal(records.length, 31)
        assert.deepEqual(records.filter(doc => doc.likelyIncremental).map(doc => doc.v), ["X"])
    })

    test("1/11 X, 2/11 X X: one more X", async () => {
        await poll([rec("X")])
        await poll([rec("X"), rec("X")])
        assert.deepEqual(values(), ["X", "X"])
    })

    test("not a continuation (the order changed): by count, the copies beyond those stored", async () => {
        await poll([rec("a"), rec("b"), rec("X")])
        await poll([rec("a"), rec("X"), rec("X"), rec("b")])
        assert.deepEqual(values(), ["a", "b", "X", "X"])
        assert.equal(records[3].likelyIncremental, undefined)
        assert.equal(records[3].unverifiedAppend, undefined)
    })

    test("an answer that begins with the last stored record is a continuation, however short the overlap", async () => {
        await poll([rec("a"), rec("b"), rec("X")])
        await poll([rec("X"), rec("b"), rec("a")])
        assert.deepEqual(records.map(doc => [doc.v, !!doc.likelyIncremental]), [["a", false], ["b", false], ["X", false], ["b", true], ["a", true]])
    })

    test("not a continuation, unalignedAnswer \"append\": the whole answer appended, flagged unverifiedAppend", async () => {
        config.apiConnectorConfig.apiUrls = [{ name: "Snapshot", url: URL_A, unalignedAnswer: "append" }]
        await poll([rec("a"), rec("b")])
        await poll([rec("a"), rec("c"), rec("b")])
        assert.deepEqual(records.map(doc => [doc.v, !!doc.unverifiedAppend]), [["a", false], ["b", false], ["a", true], ["c", true], ["b", true]])
    })

    test("not a continuation, unalignedAnswer \"upsert\" (in apiConnectorConfig): replaced; a continuation is still appended", async () => {
        config.apiConnectorConfig.unalignedAnswer = "upsert"
        await poll([rec("a"), rec("b")])
        await poll([rec("b"), rec("c")]) // continues: only c
        assert.deepEqual(values(), ["a", "b", "c"])
        await poll([rec("z"), rec("a")]) // does not continue: replaced
        assert.deepEqual(values(), ["z", "a"])
    })

    test("upsertRecords: every answer replaces the records, as before (continuation or not), with datePolled", async () => {
        config.apiConnectorConfig.upsertRecords = true
        await poll([rec("a"), rec("b")])
        await poll([rec("b"), rec("c")])
        assert.deepEqual(values(), ["b", "c"])
        assert.ok(records.every(doc => doc.datePolled instanceof Date && !doc.likelyIncremental))
    })

    test("the _id of the API records dropped (as before)", async () => {
        await poll([{ _id: "remote-1", v: "a" }])
        assert.notEqual(records[0]._id, "remote-1")
        assert.equal(records[0].v, "a")
    })

    test("paginated: the pages together, duplicates of the first poll kept", async () => {
        config.apiConnectorConfig.apiUrls = [{ name: "Snapshot", url: URL_A, pagination: { offsetParam: "offset", limitParam: "limit", limit: 2, condition: r => r.data.length > 0 } }]
        await poll([rec("X"), rec("X"), rec("a")])
        assert.deepEqual(values(), ["X", "X", "a"])
        await poll([rec("X"), rec("X"), rec("a"), rec("X")])
        assert.deepEqual(values(), ["X", "X", "a", "X"])
        assert.equal(records[3].likelyIncremental, true)
    })
})
