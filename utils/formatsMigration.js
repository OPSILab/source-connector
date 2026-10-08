// One-off: the format of the Orion refs of keys / values / entries, without the (hours long) Orion rebuild.
// Used by scripts/migrateOrionFormats.js.
//
// Refs written before the formats existed have none (entriesStore: the format is where an entry was found in its
// document). The datapoints are plain objects: their refs are "object". No datapoint is read to know which origins
// are datapoints: they are the Orion origins of the keys `survey` and `dimensions` (DATAPOINT_FILTER), read from the
// keys collection (a few hundred documents).
// The other Orion origins (Orion records that are not datapoints, e.g. a GeoJSON dataset without mapping; usually
// none) are checked on one of their records (same rule as entriesStore.entriesFormat): "object" ones are set too,
// the others - or those whose record is not found within SAMPLE_TIME_MS, without an index on fromUrl - are left out
// and listed (rebuild them one by one: POST /api/rebuild?mode=entries&connector=orion&origin=<url>).
//
// checkRecords = false: every Orion ref becomes "object", nothing is checked.

const logger = require("percocologger")
const Key = require("../api/models/Key")
const Values = require("../api/models/Value")
const Entries = require("../api/models/Entries")
const { collectionModel } = require("./collections")
const { entriesFormat } = require("./entriesStore")

const NO_ORIGIN = "orion:no-origin" // rebuild.originOf: Orion records without fromUrl
const SAMPLE_TIME_MS = 30000

// Orion origins of the keys (all of them, or those of some keys)
async function orionOriginsOfKeys(keys) {
    const rows = await Key.aggregate([
        ...(keys ? [{ $match: { key: { $in: keys } } }] : []),
        { $unwind: "$refs" },
        { $match: { "refs.connector": "orion" } },
        { $group: { _id: { origin: "$refs.origin", key: "$key" } } }
    ])
    const byKey = new Map()
    for (const { _id } of rows)
        (byKey.get(_id.key) || byKey.set(_id.key, new Set()).get(_id.key)).add(_id.origin)
    return byKey
}

// { datapoints: [origins], others: { origin: format | "not found" } }
async function classifyOrionOrigins() {
    const all = new Set([...(await orionOriginsOfKeys()).values()].flatMap(set => [...set]))
    const marked = await orionOriginsOfKeys(["survey", "dimensions"])
    const survey = marked.get("survey") || new Set()
    const datapoints = [...(marked.get("dimensions") || [])].filter(o => survey.has(o))
    const others = {}
    for (const origin of all) {
        if (datapoints.includes(origin))
            continue
        try {
            const filter = origin == NO_ORIGIN ? { fromUrl: { $exists: false } } : { fromUrl: origin }
            const record = await collectionModel("orion").collection.findOne(filter, { maxTimeMS: SAMPLE_TIME_MS })
            others[origin] = record ? entriesFormat(record) : "not found"
        }
        catch (error) {
            logger.warn(`Orion origin ${origin}: no record read within ${SAMPLE_TIME_MS} ms`, error.message)
            others[origin] = "not found"
        }
    }
    return { datapoints, others }
}

async function migrateOrionFormats({ dryRun = false, checkRecords = true } = {}) {
    const classified = checkRecords ? await classifyOrionOrigins() : { datapoints: [], others: {} }
    const skipped = Object.fromEntries(Object.entries(classified.others).filter(([, format]) => format != "object"))
    const excluded = Object.keys(skipped)
    const untagged = { refs: { $elemMatch: { connector: "orion", format: { $exists: false }, origin: { $nin: excluded } } } }
    const stats = { datapointOrigins: classified.datapoints.length, otherOrigins: classified.others, originsLeftOut: skipped, documents: {} }
    for (const Model of [Entries, Key, Values]) {
        if (dryRun) {
            stats.documents[Model.modelName] = await Model.collection.countDocuments(untagged)
            continue
        }
        const { modifiedCount } = await Model.collection.updateMany(untagged, [
            {
                $set: {
                    refs: {
                        $map: {
                            input: "$refs",
                            in: {
                                $cond: [
                                    {
                                        $and: [
                                            { $eq: ["$$this.connector", "orion"] },
                                            { $not: [{ $ifNull: ["$$this.format", false] }] },
                                            { $not: [{ $in: ["$$this.origin", excluded] }] }
                                        ]
                                    },
                                    // format last: the field order of entriesStore.ref (whole refs are compared)
                                    { $mergeObjects: ["$$this", { format: "object" }] },
                                    "$$this"
                                ]
                            }
                        }
                    }
                }
            },
            // the union of the refs' formats, as entriesStore keeps it
            { $set: { formats: { $setUnion: [{ $filter: { input: "$refs.format", cond: { $ne: ["$$this", null] } } }, []] } } }
        ])
        stats.documents[Model.modelName] = modifiedCount
        logger.info(`${Model.modelName}: Orion refs set to "object" in ${modifiedCount} documents`)
    }
    return stats
}

module.exports = { migrateOrionFormats, classifyOrionOrigins }
