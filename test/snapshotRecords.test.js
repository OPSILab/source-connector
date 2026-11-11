// utils/snapshotRecords.js: what the answer of a non incremental poll adds to the records stored for its url.
const { load } = require("./helpers/env")
const { test, describe } = require("node:test")
const assert = require("node:assert/strict")
const { recordKey, alignByOrder, countDelta } = load("utils/snapshotRecords.js")

// records "a", "b", ...: their keys are themselves
const range = (n, prefix = "r") => Array.from({ length: n }, (_, i) => prefix + i)
const insertedBy = (stored, incoming) => {
    const overlap = alignByOrder(stored, incoming)
    return overlap !== null ? { order: incoming.slice(overlap) } : { count: countDelta(stored, incoming).map(i => incoming[i]) }
}

describe("recordKey", () => {
    test("the content only: _id, datePolled and the flags ignored, keys in any order, nested too", () => {
        const a = { _id: "1", datePolled: new Date(), likelyIncremental: true, b: 1, a: { y: 2, x: [1, { q: 1, p: 2 }] } }
        const b = { a: { x: [1, { p: 2, q: 1 }], y: 2 }, b: 1, unverifiedAppend: true }
        assert.equal(recordKey(a), recordKey(b))
        assert.notEqual(recordKey(a), recordKey({ ...b, b: 2 }))
        assert.notEqual(recordKey({ a: [1, 2] }), recordKey({ a: [2, 1] })) // the order of an array counts
    })
})

describe("the cases of a non incremental API (X: a record repeated)", () => {
    const thirty = [...range(27), "X", "X", "X"] // first poll: 30 records, 3 the same

    test("1/11 X, 2/11 X X: one more X", () => {
        assert.deepEqual(insertedBy(["X"], ["X", "X"]), { order: ["X"] })
    })

    test("the same 30 again: nothing", () => {
        assert.deepEqual(insertedBy(thirty, thirty), { order: [] })
    })

    test("31 records, a fourth X at the end: that X", () => {
        assert.deepEqual(insertedBy(thirty, [...thirty, "X"]), { order: ["X"] })
    })

    test("the window slid by one: the first record out, a fourth X in: that X", () => {
        assert.deepEqual(insertedBy(thirty, [...thirty.slice(1), "X"]), { order: ["X"] })
    })

    test("the window slid while the first records were X: the X that went out does not hide the new one", () => {
        const stored = ["X", "X", "X", ...range(27)]
        // the first X went out, a new X came in at the end: the same multiset, told apart by the order
        assert.deepEqual(insertedBy(stored, [...stored.slice(1), "X"]), { order: ["X"] })
        assert.deepEqual(countDelta(stored, [...stored.slice(1), "X"]), []) // by count alone: nothing
    })

    test("the first record of the answer stored more than once: the occurrence whose tail fits", () => {
        // "a" at 0 (tail of 6, longer than the answer) and at 4 (tail of 2, the beginning of the answer)
        assert.deepEqual(insertedBy(["a", "b", "c", "d", "a", "e"], ["a", "e", "f"]), { order: ["f"] })
    })

    test("an occurrence whose tail does not match the answer: the next one; none: by count", () => {
        assert.deepEqual(insertedBy(["a", "b", "a", "c"], ["a", "c", "d"]), { order: ["d"] })
        // shuffled: no occurrence fits, by count: only the copies beyond those stored
        assert.deepEqual(insertedBy(["a", "b", "c", "X"], ["c", "a", "X", "X", "b"]), { count: ["X"] })
    })

    test("the first record of the answer never stored: by count", () => {
        assert.deepEqual(insertedBy(["a", "b"], ["z", "a", "b", "b"]), { count: ["z", "b"] })
    })
})

describe("countDelta", () => {
    test("the copies beyond those stored, in the order of the answer", () => {
        assert.deepEqual(countDelta(["X", "a", "X"], ["a", "X", "X", "X", "b", "a"]), [3, 4, 5])
    })
})
