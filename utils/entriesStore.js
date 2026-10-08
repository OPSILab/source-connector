// Keys / Values / Entries store with per-origin references.
//
// Document shape (Entries; Key has only `key`, Values only `value`):
//   { key, value, visibility: ["public-data"], connectors: ["api"], formats: ["object"],
//     refs: [{ origin: "<url | file>", visibility: "public-data", connector: "api", format: "object" }] }
// connector: api | orion | minio (utils/collections.js), so that the suggestions follow the collections searched;
// `connectors` is the union of the refs' connectors (like `visibility`), recomputed when an origin is removed.
// format: where the entry was found in its document, i.e. the Advanced search file type that finds it (FORMATS,
// entriesFormat): object (top-level fields: JSON), json (rows of a JSON array: JSON), csv (CSV rows), geojson
// (properties of the features: GeoJSON). `formats` is their union, like `connectors`. Refs written before formats
// existed have none: the rebuild (or the MinIO sync) writes them again.
// Refs written before connectors existed have none: the rebuild (utils/rebuild.js) replaces them.
// Keys whose values are not indexed for some origins (datapointRules.valuesNotIndexed, e.g. the datapoints' `value`)
// also have valuesNotIndexed: ["<origin>", ...]: the Query-Engine tells the user their values are not suggested.
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
const { valuesNotIndexed } = require('./datapointRules')
const { CONNECTORS } = require('./collections')

const BULK_CHUNK_SIZE = 1000      // operations per bulkWrite
const COLLECTOR_CHUNK_ITEMS = 500 // items accumulated in RAM before a flush

const HAS_REFS = { "refs.origin": { $exists: true } }
const FORMATS = ["object", "json", "csv", "geojson"]

// The format of the entries of a document - the same cases as common.getEntries
function entriesFormat(doc) {
  if (doc?.csv)
    return "csv"
  if (Array.isArray(doc?.json))
    return "json"
  if (doc?.features)
    return "geojson"
  return "object"
}
// `connectors` / `formats` recomputed from the refs (left out when no ref has one: legacy docs)
const unionOfRefs = field => ({
  $cond: [{ $gt: [{ $size: { $setUnion: ["$refs." + field, []] } }, 0] }, { $setUnion: ["$refs." + field, []] }, "$$REMOVE"]
})
const CONNECTORS_OF_REFS = unionOfRefs("connector")
const FORMATS_OF_REFS = unionOfRefs("format")

let indexesPromise
function ensureIndexes() {
  if (!indexesPromise)
    indexesPromise = Promise.all([
      Entries.collection.createIndex({ key: 1, value: 1 }),
      Entries.collection.createIndex({ "refs.origin": 1 }),
      Key.collection.createIndex({ key: 1 }),
      Key.collection.createIndex({ "refs.origin": 1 }),
      Values.collection.createIndex({ value: 1 }),
      Values.collection.createIndex({ "refs.origin": 1 }),
      // suggestions of some collections only
      Entries.collection.createIndex({ connectors: 1, key: 1, value: 1 }),
      Key.collection.createIndex({ connectors: 1, key: 1 }),
      Values.collection.createIndex({ connectors: 1, value: 1 })
    ]).catch(error => {
      indexesPromise = undefined // retry at next call
      logger.error("Error creating indexes for keys/values/entries", error)
    })
  return indexesPromise
}

function ref(origin, visibility, connector, format) {
  // field order must stay the same (origin, visibility, connector, format): $addToSet compares whole objects
  const r = { origin, visibility }
  if (connector !== undefined)
    r.connector = connector
  if (format !== undefined)
    r.format = format
  return r
}

function upsertOp(match, visibility, origin, connector, format, extra = {}) {
  return {
    updateOne: {
      filter: { ...match, ...HAS_REFS },
      update: {
        $addToSet: {
          visibility: { $each: visibility },
          refs: { $each: visibility.map(v => ref(origin, v, connector, format)) },
          ...(connector === undefined ? {} : { connectors: connector }),
          ...(format === undefined ? {} : { formats: format }),
          ...extra
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
// keysOnly = { [key]: [visibility, ...] }: keys whose values are not indexed (no values / entries for them)
// connector: api | orion | minio (undefined only in old callers / tests: refs without connector)
// format: object | json | csv | geojson, see entriesFormat (undefined only in old callers / tests)
async function writeAccumulator(acc, origin, keysOnly = {}, connector, format) {
  if (connector !== undefined && !CONNECTORS.includes(connector))
    throw new Error(`Unknown connector "${connector}"`)
  if (format !== undefined && !FORMATS.includes(format))
    throw new Error(`Unknown format "${format}"`)
  await ensureIndexes()
  const entryOps = []
  const keyOps = []
  const valuesVisibility = new Map()

  for (const key in acc) {
    const keyVisibility = new Set()
    for (const value in acc[key]) {
      const visibility = acc[key][value]
      entryOps.push(upsertOp({ key, value }, visibility, origin, connector, format))
      let valueVisibility = valuesVisibility.get(value)
      if (!valueVisibility)
        valuesVisibility.set(value, valueVisibility = new Set())
      for (const v of visibility) {
        keyVisibility.add(v)        // union over all values of the key
        valueVisibility.add(v)      // union over all keys holding the value
      }
    }
    keyOps.push(upsertOp({ key }, [...keyVisibility], origin, connector, format))
  }
  for (const key in keysOnly)
    keyOps.push(upsertOp({ key }, keysOnly[key], origin, connector, format, { valuesNotIndexed: origin }))
  const valueOps = [...valuesVisibility].map(([value, visibility]) => upsertOp({ value }, [...visibility], origin, connector, format))

  await bulkInChunks(Entries, entryOps)
  await bulkInChunks(Key, keyOps)
  await bulkInChunks(Values, valueOps)
  logger.info(`Keys/values/entries written for ${origin}${format ? " (" + format + ")" : ""}: ${keyOps.length} keys, ${valueOps.length} values, ${entryOps.length} entries`)
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
      { $set: { visibility: { $setUnion: ["$refs.visibility", []] }, connectors: CONNECTORS_OF_REFS, formats: FORMATS_OF_REFS } },
      ...(Model === Key ? [{
        $set: {
          valuesNotIndexed: {
            $cond: [{ $isArray: "$valuesNotIndexed" }, { $filter: { input: "$valuesNotIndexed", cond: { $ne: ["$$this", origin] } } }, "$$REMOVE"]
          }
        }
      }] : [])
    ])
    logger.debug(`${Model.modelName}: removed origin ${origin} (${deleted.deletedCount} deleted, ${updated.modifiedCount} updated)`)
  }
}

// One collector per origin: add() only computes entries in RAM, the DB is written every
// COLLECTOR_CHUNK_ITEMS items and on the final flush().
function createCollector(origin, connector) {
  let byFormat = {} // format -> { acc, keysOnly }: the refs of each format are written separately
  let pending = 0
  return {
    async add(items) {
      for (const item of items) {
        const { _id, ...data } = item // _id is a mongo id, not a data field
        const { acc, keysOnly } = byFormat[entriesFormat(data)] ||= { acc: {}, keysOnly: {} }
        if (connector == "orion")
          delete data.dupl_hash // the Datapoint dedup key (the whole document as JSON): one value per datapoint
        // fields whose values are not indexed: only their key (same visibility as getEntries without a name)
        for (const field of valuesNotIndexed(data, connector)) {
          delete data[field]
          keysOnly[field] = ["public-data"]
        }
        await getEntries([data], "json", undefined, acc) // name undefined -> "public-data"
        if (++pending >= COLLECTOR_CHUNK_ITEMS)
          await this.flush()
      }
    },
    async flush() {
      if (!pending)
        return
      const snapshot = byFormat
      byFormat = {}
      pending = 0
      for (const [format, { acc, keysOnly }] of Object.entries(snapshot))
        await writeAccumulator(acc, origin, keysOnly, connector, format)
    }
  }
}

// Refs written before connectors existed (no `connector`) or by a connector given in `connectors`: dropped, and
// the documents left without refs deleted. Used by the rebuild before writing those connectors' refs again.
async function removeRefs({ withoutConnector = false, connectors = [] } = {}) {
  await ensureIndexes()
  const conditions = []
  if (withoutConnector)
    conditions.push({ $not: [{ $ifNull: ["$$this.connector", false] }] })
  if (connectors.length)
    conditions.push({ $in: ["$$this.connector", connectors] })
  if (!conditions.length)
    return
  const doomed = { $or: conditions }
  const match = { $or: [...(withoutConnector ? [{ refs: { $elemMatch: { connector: { $exists: false } } } }] : []), ...(connectors.length ? [{ "refs.connector": { $in: connectors } }] : [])] }
  for (const Model of [Entries, Key, Values]) {
    const updated = await Model.collection.updateMany({ ...HAS_REFS, ...match }, [
      { $set: { refs: { $filter: { input: "$refs", cond: { $not: [doomed] } } } } },
      { $set: { visibility: { $setUnion: ["$refs.visibility", []] }, connectors: CONNECTORS_OF_REFS, formats: FORMATS_OF_REFS } },
      ...(Model === Key ? [{
        $set: {
          // origins of removed refs: valuesNotIndexed keeps only those still referenced
          valuesNotIndexed: {
            $cond: [{ $isArray: "$valuesNotIndexed" }, { $filter: { input: "$valuesNotIndexed", cond: { $in: ["$$this", "$refs.origin"] } } }, "$$REMOVE"]
          }
        }
      }] : [])
    ])
    const deleted = await Model.collection.deleteMany({ refs: { $size: 0 } }) // no `refs` at all: legacy docs, untouched
    logger.info(`${Model.modelName}: refs removed from ${updated.modifiedCount} docs, ${deleted.deletedCount} docs left without refs deleted`)
  }
}

module.exports = {
  FORMATS,
  entriesFormat,
  createCollector,
  removeRefs,
  removeOrigin,
  writeAccumulator,
  ensureIndexes
}
