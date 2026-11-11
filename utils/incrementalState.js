// The state of the incremental polls of a url (an API, or one element of a batch API), in MongoDB (collection
// "incrementalstate", whatever collections.api says: like the status log and the log of the resets):
//
//   lastEnd    the end of the last window stored. Every window of a poll (incrementalParams.maxWindow) is saved as
//              soon as it is complete, and the next poll starts from lastEnd: a window already stored is never
//              requested again, also when the poll that stored it stopped before its last window (an error, an
//              expired token, a restart). A window answered 404 counts as stored (an empty answer).
//   complete   the poll that wrote lastEnd reached now (its last window): a second poll on the same day is skipped.
//              false: the poll was interrupted, the next one resumes from lastEnd even on the same day.
//   missed     the windows answered 404, with incrementalParams.retryNotFound: requested again, before the new ones,
//              at every poll, until they answer.
//
// Without a state (urls polled before it existed) the polls start from the last record (datePolled), as before.
const IncrementalState = require("../api/models/IncrementalState")

const sameParams = (a, b) => JSON.stringify(a) === JSON.stringify(b)

async function get(url) {
    return IncrementalState.collection.findOne({ _id: url })
}

// A window is stored (`last`: it was the last one of the poll)
async function windowDone(url, end, last) {
    await IncrementalState.collection.updateOne({ _id: url }, { $set: { lastEnd: new Date(end), complete: !!last } }, { upsert: true })
}

async function addMissed(url, params) {
    const state = await get(url)
    if ((state?.missed || []).some(missed => sameParams(missed.params, params)))
        return
    await IncrementalState.collection.updateOne({ _id: url }, { $push: { missed: { params, at: new Date() } } }, { upsert: true })
}

async function removeMissed(url, params) {
    const state = await get(url)
    const missed = (state?.missed || []).filter(entry => !sameParams(entry.params, params))
    await IncrementalState.collection.updateOne({ _id: url }, { $set: { missed } })
}

// The reset of an API: its state deleted (urls: the API, or the elements of a batch API)
async function clear(urls) {
    await IncrementalState.collection.deleteMany({ _id: { $in: urls } })
}

// The urls with a state that match a regular expression (the elements of a batch API)
async function urlsMatching(pattern) {
    return (await IncrementalState.collection.find({ _id: { $regex: pattern } }, { projection: { _id: 1 } }).toArray()).map(doc => doc._id)
}

module.exports = { get, windowDone, addMissed, removeMissed, clear, urlsMatching }
