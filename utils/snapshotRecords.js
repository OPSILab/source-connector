// Non incremental API polls: what a new answer adds to the records already stored for its url (inputConnectors/
// apiConnector.js, storeSnapshot). The records are compared by content: every field except _id, datePolled and the
// flags below, whatever the order of the keys.
//
// 1. By order. The answer is taken as the continuation of the stored records (an API that returns a window sliding
//    over its data, oldest first): its first record is looked for among the stored ones, from the oldest; from that
//    occurrence the stored records to the end must be the beginning of the answer (and not longer than it), and the
//    rest of the answer is new. An occurrence that does not fit: the next one. The new records get likelyIncremental.
// 2. Otherwise the order says nothing (the data was shuffled, replaced, or records removed here and there); by
//    configuration, unalignedAnswer (of the API, else of apiConnectorConfig):
//      "upsert"   the records of the url replaced by the answer
//      "append"   the whole answer appended, flagged unverifiedAppend (maybe duplicates, interval unknown)
//      not set    by count: of each record, the copies beyond those already stored
// The first answer of a url (nothing stored yet) is stored whole. With upsertRecords none of this: the records of the
// url are replaced by every answer, as before.
const FLAGS = { likelyIncremental: "likelyIncremental", unverifiedAppend: "unverifiedAppend" }
const IGNORED = new Set(["_id", "datePolled", ...Object.values(FLAGS)])

function canonical(value) {
    if (Array.isArray(value))
        return value.map(canonical)
    if (value instanceof Date)
        return value.toISOString()
    if (value !== null && typeof value === "object") {
        if (typeof value.toHexString === "function")
            return value.toHexString()
        const out = {}
        for (const key of Object.keys(value).sort())
            if (value[key] !== undefined)
                out[key] = canonical(value[key])
        return out
    }
    return value
}

// The content of a record, to compare it with others
function recordKey(record) {
    const content = {}
    for (const key of Object.keys(record || {}))
        if (!IGNORED.has(key))
            content[key] = record[key]
    return JSON.stringify(canonical(content))
}

// 1: how many records at the beginning of the answer are the last stored ones (the rest is new), or null
function alignByOrder(stored, incoming) {
    if (!stored.length || !incoming.length)
        return null
    const first = incoming[0]
    for (let i = stored.indexOf(first); i !== -1; i = stored.indexOf(first, i + 1)) {
        const overlap = stored.length - i
        if (overlap > incoming.length)
            continue
        let same = true
        for (let k = 1; k < overlap && same; k++)
            same = stored[i + k] === incoming[k]
        if (same)
            return overlap
    }
    return null
}

// 2, neither: the positions of the records of the answer to insert (the copies beyond those already stored)
function countDelta(stored, incoming) {
    const counts = new Map()
    for (const key of stored)
        counts.set(key, (counts.get(key) || 0) + 1)
    const insert = []
    incoming.forEach((key, i) => {
        const left = counts.get(key) || 0
        if (left > 0)
            counts.set(key, left - 1)
        else
            insert.push(i)
    })
    return insert
}

module.exports = { FLAGS, recordKey, alignByOrder, countDelta }
