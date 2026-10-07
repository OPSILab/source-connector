// Fields of the Orion records (datapoints) whose values are not indexed in values / entries (the key is): with tens
// of millions of datapoints a measure like `value` has hundreds of thousands of distinct values.
// orion.datapointsNotIndexed, ["value"] when not configured. They stay searchable (Advanced search, GraphQL): only
// the suggestions leave them out, and the Query-Engine tells the user (GET /api/keys/notIndexed).

const config = require("../config")

// Datapoints among the documents of the Orion collection (which also holds the Orion records that are not
// datapoints): used by the Query-Engine's GraphQL datapoints
const DATAPOINT_FILTER = { survey: { $exists: true }, dimensions: { $exists: true } }

function datapointsNotIndexed() {
    const fields = config.orion?.datapointsNotIndexed
    return Array.isArray(fields) ? fields.filter(f => typeof f === "string" && f) : ["value"]
}

// The top-level fields of `doc` whose values are not indexed: only for the Orion connector
function valuesNotIndexed(doc, connector) {
    if (connector !== "orion" || doc === null || typeof doc !== "object")
        return []
    return datapointsNotIndexed().filter(field => doc[field] !== undefined)
}

module.exports = { DATAPOINT_FILTER, datapointsNotIndexed, valuesNotIndexed }
