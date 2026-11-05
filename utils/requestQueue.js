// Runs the Orion notifications one at a time, in arrival order.
//
// The previous queue (an array plus a polling loop) had two bugs:
//  - when a notification threw (e.g. a mapping error), it was never removed from the array: every following
//    notification waited forever ("the queue does not go on after mapping errors");
//  - with three or more notifications waiting, the third one woke up while the second was still running and ran
//    the second one's request again, in parallel.
// Here every job starts when the previous one has settled (fulfilled or rejected): an error is that notification's
// result only, the queue goes on.
//
// describe(...args) (optional): what a job is about (e.g. the datasets of a notification), listed by jobs() with when
// it arrived and, for the one running, when it started. A describe that throws leaves the job without description.

function createQueue(worker, { describe } = {}) {
    let tail = Promise.resolve()
    const jobs = [] // waiting or running, in arrival order: { receivedAt, startedAt, info }
    const infoOf = args => {
        try {
            return describe ? describe(...args) : undefined
        }
        catch {
            return undefined
        }
    }
    return {
        // the notifications waiting or running
        size: () => jobs.length,
        // the same, in arrival order: { receivedAt, startedAt (the running one only), ...info }
        jobs: () => jobs.map(job => ({ receivedAt: job.receivedAt, ...(job.startedAt ? { startedAt: job.startedAt } : {}), ...job.info })),
        // resolves with the worker's result; rejects with its error (the queue goes on anyway)
        push(...args) {
            const job = { receivedAt: new Date(), info: infoOf(args) }
            jobs.push(job)
            const run = tail.then(() => {
                job.startedAt = new Date()
                return worker(...args)
            })
            const done = run.finally(() => { jobs.splice(jobs.indexOf(job), 1) })
            tail = done.catch(() => { })
            return done
        }
    }
}

module.exports = { createQueue }
