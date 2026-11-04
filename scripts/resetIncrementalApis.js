// CLI wrapper of utils/resetIncremental.js (same logic as POST /api/incremental/reset): deletes the records of the
// incremental APIs (MongoDB, PostgreSQL, keys/values/entries, pending polls) and polls them again from the start
// (startDateValue, maxWindow windows, every page).
//
//   node scripts/resetIncrementalApis.js [--name <api name>]... [--dry-run]
//
// No --name: every incremental API of apiConnectorConfig.apiUrls. --dry-run: only how many records each one has.
//
// The in-process lock does not protect a separate CLI process: stop the Source-Connector (or disable its
// apiConnector) while this runs, or prefer the HTTP endpoint on the running service.

process.percocologger = require("../percocologger.config")
const common = require("../utils/common")
const config = common.checkConfig(require("../config"), require("../config.template"))
require("../utils/collections").validateCollections()
const mongoose = require("mongoose")
const logger = require("percocologger")
const locks = require("../utils/jobLocks")
const { resetIncremental } = require("../utils/resetIncremental")

// apiConnector polls every API as soon as it is loaded and then at every pollInterval: here only the reset polls
function loadPollAPI() {
    const apis = config.apiConnectorConfig.apiUrls
    config.apiConnectorConfig.apiUrls = []
    const realSetInterval = global.setInterval
    global.setInterval = () => ({ unref() { } })
    try {
        return require("../inputConnectors/apiConnector").pollAPI
    }
    finally {
        global.setInterval = realSetInterval
        config.apiConnectorConfig.apiUrls = apis
    }
}

async function main() {
    const args = process.argv.slice(2)
    const names = []
    for (let i = 0; i < args.length; i++)
        if (args[i] == "--name") {
            if (!args[i + 1] || args[i + 1].startsWith("--"))
                throw new Error("--name requires a value")
            names.push(args[++i])
        }
        else if (args[i] != "--dry-run")
            throw new Error(`Unknown argument ${args[i]}. Usage: node scripts/resetIncrementalApis.js [--name <api name>]... [--dry-run]`)
    const dryRun = args.includes("--dry-run")

    await mongoose.connect(config.mongo)
    try {
        const pollAPI = dryRun ? undefined : loadPollAPI()
        while (locks.polling) // the (empty) poll started by apiConnector when loaded
            await new Promise(resolve => setTimeout(resolve, 50))
        console.log(JSON.stringify(await resetIncremental({ names, pollAPI, dryRun, by: "script (scripts/resetIncrementalApis.js)" }), null, 2))
    }
    finally {
        await mongoose.disconnect()
    }
}

main()
    .then(() => process.exit(0))
    .catch(error => {
        logger.error(error)
        console.error(error.message || error)
        process.exit(1)
    })
