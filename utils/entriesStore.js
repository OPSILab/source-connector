// Keys / Values / Entries store with per-origin references.
//
// Document shape (Entries; Key has only `key`, Values only `value`):
//   { key, value, visibility: ["public-data"], refs: [{ origin: "<url | file>", visibility: "public-data" }] }
//
// - Upsert + $addToSet: idempotent, a block can be written twice without side effects.
// - Docs without `refs` (legacy, written before refs existed) are never matched nor modified;
//   minioConnector.sync() deletes them and rebuilds them with refs (origin "minio://<bucket>/<key>").
// - Native driver (Model.collection) on purpose: Key / Value schemas are strict and
//   mongoose would strip `refs` from the update.

const logger = require('percocologger')
const Key = require('../api/models/Key')
const Values = require('../api/models/Value')
const Entries = require('../api/models/Entries')
const { getEntries } = require('./common')

const BULK_CHUNK_SIZE = 1000      // operations per bulkWrite
const COLLECTOR_CHUNK_ITEMS = 500 // items accumulated in RAM before a flush

const HAS_REFS = { "refs.origin": { $exists: true } }

let indexesPromise
function ensureIndexes() {
  if (!indexesPromise)
    indexesPromise = Promise.all([
      Entries.collection.createIndex({ key: 1, value: 1 }),
      Entries.collection.createIndex({ "refs.origin": 1 }),
      Key.collection.createIndex({ key: 1 }),
      Key.collection.createIndex({ "refs.origin": 1 }),
      Values.collection.createIndex({ value: 1 }),
      Values.collection.createIndex({ "refs.origin": 1 })
    ]).catch(error => {
      indexesPromise = undefined // retry at next call
      logger.error("Error creating indexes for keys/values/entries", error)
    })
  return indexesPromise
}

function upsertOp(match, visibility, origin) {
  return {
    updateOne: {
      filter: { ...match, ...HAS_REFS },
      update: {
        $addToSet: {
          visibility: { $each: visibility },
          // field order must stay the same (origin, visibility): $addToSet compares whole objects
          refs: { $each: visibility.map(v => ({ origin, visibility: v })) }
        }
      },
      upsert: true
    }
  }
}

async function bulkInChunks(Model, ops) {
  for (let i = 0; i < ops.length; i += BULK_CHUNK_SIZE)
    try {
      await Model.collection.bulkWrite(ops.slice(i, i + BULK_CHUNK_SIZE), { ordered: false })
    }
    catch (error) {
      if (!error?.writeErrors)
        throw error // connection / server error: not a per-document failure
      logger.error(`${Model.modelName}: ${error.writeErrors.length} failed operations in block starting at ${i}`, error.writeErrors[0]?.errmsg)
    }
}

// acc = { [key]: { [stringifiedValue]: [visibility, ...] } }  (same shape produced by common.getEntries)
async function writeAccumulator(acc, origin) {
  await ensureIndexes()
  const entryOps = []
  const keyOps = []
  const valuesVisibility = new Map()

  for (const key in acc) {
    const keyVisibility = new Set()
    for (const value in acc[key]) {
      const visibility = acc[key][value]
      entryOps.push(upsertOp({ key, value }, visibility, origin))
      let valueVisibility = valuesVisibility.get(value)
      if (!valueVisibility)
        valuesVisibility.set(value, valueVisibility = new Set())
      for (const v of visibility) {
        keyVisibility.add(v)        // union over all values of the key
        valueVisibility.add(v)      // union over all keys holding the value
      }
    }
    keyOps.push(upsertOp({ key }, [...keyVisibility], origin))
  }
  const valueOps = [...valuesVisibility].map(([value, visibility]) => upsertOp({ value }, [...visibility], origin))

  await bulkInChunks(Entries, entryOps)
  await bulkInChunks(Key, keyOps)
  await bulkInChunks(Values, valueOps)
  logger.info(`Keys/values/entries written for ${origin}: ${keyOps.length} keys, ${valueOps.length} values, ${entryOps.length} entries`)
}

// Removes every reference to `origin`; docs left without references are deleted.
// Used only with upsertRecords (the origin is rebuilt right after).
async function removeOrigin(origin) {
  await ensureIndexes()
  for (const Model of [Entries, Key, Values]) {
    // 1. docs referenced only by this origin -> delete (uses the refs.origin index)
    const deleted = await Model.collection.deleteMany({
      "refs.origin": origin,
      refs: { $not: { $elemMatch: { origin: { $ne: origin } } } }
    })
    // 2. docs shared with other origins -> drop this origin and recompute visibility
    const updated = await Model.collection.updateMany({ "refs.origin": origin }, [
      { $set: { refs: { $filter: { input: "$refs", cond: { $ne: ["$$this.origin", origin] } } } } },
      { $set: { visibility: { $setUnion: ["$refs.visibility", []] } } }
    ])
    logger.debug(`${Model.modelName}: removed origin ${origin} (${deleted.deletedCount} deleted, ${updated.modifiedCount} updated)`)
  }
}

// One collector per origin: add() only computes entries in RAM, the DB is written every
// COLLECTOR_CHUNK_ITEMS items and on the final flush().
function createCollector(origin) {
  let acc = {}
  let pending = 0
  return {
    async add(items) {
      for (const item of items) {
        const { _id, ...data } = item // _id is a mongo id, not a data field
        await getEntries([data], "json", undefined, acc) // name undefined -> "public-data"
        if (++pending >= COLLECTOR_CHUNK_ITEMS)
          await this.flush()
      }
    },
    async flush() {
      if (!pending)
        return
      const snapshot = acc
      acc = {}
      pending = 0
      await writeAccumulator(snapshot, origin)
    }
  }
}

module.exports = {
  createCollector,
  removeOrigin,
  writeAccumulator,
  ensureIndexes
}
