// Runs the Orion notifications one at a time, in arrival order.
//
// The previous queue (an array plus a polling loop) had two bugs:
//  - when a notification threw (e.g. a mapping error), it was never removed from the array: every following
//    notification waited forever ("the queue does not go on after mapping errors");
//  - with three or more notifications waiting, the third one woke up while the second was still running and ran
//    the second one's request again, in parallel.
// Here every job starts when the previous one has settled (fulfilled or rejected): an error is that notification's
// result only, the queue goes on.

function createQueue(worker) {
    let tail = Promise.resolve()
    let pending = 0
    return {
        // the notifications waiting or running
        size: () => pending,
        // resolves with the worker's result; rejects with its error (the queue goes on anyway)
        push(...args) {
            pending++
            const run = tail.then(() => worker(...args))
            const done = run.finally(() => { pending-- })
            tail = done.catch(() => { })
            return done
        }
    }
}

module.exports = { createQueue }
