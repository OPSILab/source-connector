// Orion notification queue (utils/requestQueue.js): no database needed.
require("./helpers/env")
const { test, describe } = require("node:test")
const assert = require("node:assert/strict")
const { createQueue } = require("../utils/requestQueue")

const tick = ms => new Promise(resolve => setTimeout(resolve, ms))

describe("createQueue", () => {
    test("one job at a time, in arrival order, each with its own arguments", async () => {
        const log = []
        let running = 0, maxRunning = 0
        const queue = createQueue(async (name, ms) => {
            running++
            maxRunning = Math.max(maxRunning, running)
            log.push("start " + name)
            await tick(ms)
            log.push("end " + name)
            running--
            return name
        })
        // the old queue ran "b" twice when three were waiting
        const results = await Promise.all([queue.push("a", 20), queue.push("b", 5), queue.push("c", 1), queue.push("d", 1)])
        assert.deepEqual(results, ["a", "b", "c", "d"])
        assert.equal(maxRunning, 1)
        assert.deepEqual(log, ["start a", "end a", "start b", "end b", "start c", "end c", "start d", "end d"])
    })

    test("an error ends that job only: the next ones go on", async () => {
        const queue = createQueue(async name => {
            await tick(1)
            if (name == "broken")
                throw new Error("mapping error")
            return name
        })
        const broken = queue.push("broken")
        const next = queue.push("next")
        await assert.rejects(broken, /mapping error/)
        assert.equal(await next, "next")
        assert.equal(await queue.push("after"), "after")
    })

    test("size: the jobs waiting or running", async () => {
        let release
        const queue = createQueue(() => new Promise(resolve => { release = resolve }))
        const first = queue.push()
        queue.push()
        assert.equal(queue.size(), 2)
        await tick(1)
        release()
        await first
        await tick(1)
        assert.equal(queue.size(), 1)
        release()
        await tick(1)
        assert.equal(queue.size(), 0)
    })
})
