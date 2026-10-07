// Datapoints of the Orion datasets (data model mapper output, or a { data: { datapoints } } payload): written to
// the Orion collection (collections.orion.mongo, "datapoints") as before, with the legacy Datapoint model:
// upsertMany by dupl_hash, nothing deleted when a dataset changes. The dataset url is `fromUrl` (the origin of
// their keys / values / entries refs), `source` is the provider (EUROSTAT, ...).
// PostgreSQL (collections.orion.toPostgres, off by default: tens of millions of rows): the rows of the dataset are
// replaced, table collections.orion.postgres.

const logger = require("percocologger")
const { collectionSettings, collectionModel } = require("./collections")
const { createCollector } = require("./entriesStore")
const { insertToPostgre, deletePostgresOrigin } = require("./sourceRecords")
const { DATAPOINT_FILTER } = require("./datapointRules")

// As the mapper sends them: its own _id is not ours (and $set on _id fails on an existing document)
function cleanDatapoint(datapoint, origin) {
    const { _id, ...d } = datapoint
    if (d.fromUrl === undefined && origin !== undefined)
        d.fromUrl = origin
    return d
}

// Writer for the datapoints of one dataset, block by block (the chunks of the mapper): add(datapoints), close()
async function openDatapointsWriter(origin, { logName = origin, sqlName } = {}) {
    const settings = collectionSettings("orion")
    const Datapoint = collectionModel("orion")
    const collector = settings.toMongo ? createCollector(origin, "orion") : undefined
    let pgCleared = false
    let count = 0
    return {
        async add(datapoints) {
            const docs = datapoints.map(d => cleanDatapoint(d, origin))
            if (!docs.length)
                return
            if (settings.toMongo) {
                await Datapoint.upsertMany(docs)
                await collector.add(docs)
            }
            if (settings.toPostgres) {
                if (!pgCleared) {
                    await deletePostgresOrigin(origin, "orion")
                    pgCleared = true
                }
                await insertToPostgre(docs, logName, sqlName, origin, "orion")
            }
            count += docs.length
        },
        async close() {
            await collector?.flush()
            logger.info(`Datapoints of ${origin}: ${count}`)
            return count
        }
    }
}

async function upsertDatapoints(datapoints, origin, options) {
    const writer = await openDatapointsWriter(origin, options)
    await writer.add(datapoints)
    return writer.close()
}

module.exports = { DATAPOINT_FILTER, cleanDatapoint, openDatapointsWriter, upsertDatapoints }
