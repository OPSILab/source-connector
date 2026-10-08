// Datapoints of the Orion datasets (data model mapper output, or a { data: { datapoints } } payload): written to
// the Orion collection (collections.orion.mongo, "datapoints") with the legacy Datapoint model, as before:
//   config.upsertRecords   upsertMany by dupl_hash (unique index on it), nothing deleted
//   otherwise              insertMany; with replaceSurvey (the data model mapper flow) the datapoints of the survey
//                          are deleted first, at the first block - the survey is emptied and filled again
// The dataset url is `fromUrl` (the origin of their keys / values / entries refs), `source` is the provider
// (EUROSTAT, ...). When the survey is replaced, the refs of the dataset are removed too (its old values).
// PostgreSQL (collections.orion.toPostgres, off by default: tens of millions of rows): the rows of the dataset are
// replaced, table collections.orion.postgres.

const config = require("../config")
const logger = require("percocologger")
const { collectionSettings, collectionModel } = require("./collections")
const { createCollector, removeOrigin } = require("./entriesStore")
const { insertToPostgre, deletePostgresOrigin } = require("./sourceRecords")
const { DATAPOINT_FILTER } = require("./datapointRules")
const { cleanSurveyName } = require("../api/models/Datapoint")

// As the mapper sends them: its own _id is not ours (and $set on _id fails on an existing document)
function cleanDatapoint(datapoint, origin) {
    const { _id, ...d } = datapoint
    if (d.fromUrl === undefined && origin !== undefined)
        d.fromUrl = origin
    return d
}

// The survey as stored: by upsertMany (cleanSurveyName) or by insertMany (uppercase only, the schema's setter)
function storedSurveys(survey) {
    if (typeof survey !== "string" || !survey)
        return []
    return [...new Set([survey, survey.toUpperCase(), cleanSurveyName(survey)])]
}

// Writer for the datapoints of one dataset, block by block (the chunks of the mapper): add(datapoints), close()
async function openDatapointsWriter(origin, { logName = origin, sqlName, replaceSurvey = false } = {}) {
    const settings = collectionSettings("orion")
    const Datapoint = collectionModel("orion")
    const upsert = config.upsertRecords == true
    const collector = settings.toMongo ? createCollector(origin, "orion") : undefined
    let pgCleared = false
    let surveyReplaced = false
    let count = 0
    return {
        async add(datapoints) {
            const docs = datapoints.map(d => cleanDatapoint(d, origin))
            if (!docs.length)
                return
            if (settings.toMongo) {
                if (!upsert && replaceSurvey && !surveyReplaced) {
                    const surveys = storedSurveys(docs[0].survey)
                    if (surveys.length) {
                        const { deletedCount } = await Datapoint.collection.deleteMany({ survey: { $in: surveys } })
                        logger.info(`Datapoints of survey ${docs[0].survey} deleted before the new ones: ${deletedCount}`)
                    }
                    await removeOrigin(origin) // the suggestions of the old datapoints
                    surveyReplaced = true
                }
                if (upsert)
                    await Datapoint.upsertMany(docs)
                else
                    await Datapoint.insertMany(docs)
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
