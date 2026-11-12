// incrementalParams.alignToDays: incremental windows of whole UTC days (midnight to midnight), only the days already
// over (at now - pollDelay). prepareIncremental only: no database, no request.
const { load } = require("./helpers/env")
const { test, describe, before } = require("node:test")
const assert = require("node:assert/strict")
const { loadApiConnector } = require("./helpers/apiConnector")

const DAY = 24 * 3600 * 1000
let prepareIncremental
before(async () => {
    await loadApiConnector()
    prepareIncremental = load("inputConnectors/apiConnector.js").prepareIncremental
})

// as the NeuroFarming / 8Bells API: RFC 3339, start inclusive, end exclusive, one day per request
const rfcApi = (extra = {}) => ({
    name: "KPIs", url: "https://kpi.example/kpis", incremental: true,
    incrementalParams: {
        startDateParam: "period_start", endDateParam: "period_end", startDateFormat: "RFC 3339", endDateFormat: "RFC 3339",
        startDateLogic: "inclusive", endDateLogic: "exclusive", startDateValue: "2026-01-01T12:26:47.712923Z",
        maxWindow: DAY, alignToDays: true, ...extra
    }
})
const pairs = prepared => prepared.windows.map(w => [w.period_start, w.period_end])
const now = iso => new Date(iso)

describe("alignToDays", () => {
    test("first poll: from the midnight of startDateValue, one request per day, until the last midnight", () => {
        const prepared = prepareIncremental(undefined, rfcApi(), now("2026-01-04T16:00:00Z"), null)
        assert.equal(prepared.mustPull, true)
        assert.deepEqual(pairs(prepared), [
            ["2026-01-01T00:00:00.000Z", "2026-01-02T00:00:00.000Z"],
            ["2026-01-02T00:00:00.000Z", "2026-01-03T00:00:00.000Z"],
            ["2026-01-03T00:00:00.000Z", "2026-01-04T00:00:00.000Z"]
        ])
        assert.deepEqual(prepared.ends.map(d => d.toISOString()), ["2026-01-02T00:00:00.000Z", "2026-01-03T00:00:00.000Z", "2026-01-04T00:00:00.000Z"])
    })

    test("daily poll at 16:00: yesterday, midnight to midnight", () => {
        const state = { lastEnd: now("2026-11-11T00:00:00Z"), complete: true }
        assert.deepEqual(pairs(prepareIncremental(undefined, rfcApi(), now("2026-11-12T16:00:00Z"), state)),
            [["2026-11-11T00:00:00.000Z", "2026-11-12T00:00:00.000Z"]])
    })

    test("polled again the same day: nothing to ask", () => {
        const state = { lastEnd: now("2026-11-12T00:00:00Z"), complete: true }
        const prepared = prepareIncremental(undefined, rfcApi(), now("2026-11-12T23:59:00Z"), state)
        assert.equal(prepared.mustPull, false)
        assert.deepEqual(prepared.windows, [])
    })

    test("pollDelay of a day: on the 12th, the 10th", () => {
        const state = { lastEnd: now("2026-11-10T00:00:00Z"), complete: true }
        assert.deepEqual(pairs(prepareIncremental(undefined, rfcApi({ pollDelay: DAY }), now("2026-11-12T16:00:00Z"), state)),
            [["2026-11-10T00:00:00.000Z", "2026-11-11T00:00:00.000Z"]])
        const done = { lastEnd: now("2026-11-11T00:00:00Z"), complete: true }
        assert.equal(prepareIncremental(undefined, rfcApi({ pollDelay: DAY }), now("2026-11-12T16:00:00Z"), done).mustPull, false)
    })

    test("a state left by the old windows (afternoon): back to its midnight", () => {
        const state = { lastEnd: now("2026-11-10T16:58:32.330Z"), complete: true }
        assert.deepEqual(pairs(prepareIncremental(undefined, rfcApi(), now("2026-11-12T16:00:00Z"), state)), [
            ["2026-11-10T00:00:00.000Z", "2026-11-11T00:00:00.000Z"],
            ["2026-11-11T00:00:00.000Z", "2026-11-12T00:00:00.000Z"]
        ])
    })

    test("end inclusive: the last millisecond of the day", () => {
        const state = { lastEnd: now("2026-11-11T00:00:00Z"), complete: true }
        assert.deepEqual(pairs(prepareIncremental(undefined, rfcApi({ endDateLogic: "inclusive" }), now("2026-11-12T16:00:00Z"), state)),
            [["2026-11-11T00:00:00.000Z", "2026-11-11T23:59:59.999Z"]])
    })

    test("YYYY-MM-DD, end inclusive: the same day twice", () => {
        const api = rfcApi({ startDateParam: "date_from", endDateParam: "date_to", startDateFormat: "YYYY-MM-DD", endDateFormat: "YYYY-MM-DD", endDateLogic: "inclusive", startDateValue: "2026-11-10" })
        const prepared = prepareIncremental(undefined, api, now("2026-11-12T16:00:00Z"), null)
        assert.deepEqual(prepared.windows, [{ date_from: "2026-11-10", date_to: "2026-11-10" }, { date_from: "2026-11-11", date_to: "2026-11-11" }])
    })

    test("millis", () => {
        const api = rfcApi({ startDateFormat: "millis", endDateFormat: "millis" })
        const state = { lastEnd: now("2026-11-11T00:00:00Z"), complete: true }
        assert.deepEqual(pairs(prepareIncremental(undefined, api, now("2026-11-12T16:00:00Z"), state)),
            [[Date.parse("2026-11-11T00:00:00Z"), Date.parse("2026-11-12T00:00:00Z")]])
    })

    test("without alignToDays nothing changes: from the last end to now", () => {
        const state = { lastEnd: now("2026-11-11T16:58:32.330Z"), complete: true }
        assert.deepEqual(pairs(prepareIncremental(undefined, rfcApi({ alignToDays: undefined }), now("2026-11-12T16:57:19.702Z"), state)),
            [["2026-11-11T16:58:32.330Z", "2026-11-12T16:57:19.702Z"]])
    })
})
