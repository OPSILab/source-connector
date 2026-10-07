// CLI wrapper of utils/rebuild.js (same logic as POST /api/rebuild).
//
//   node scripts/rebuildFromSources.js <entries|postgres|all> [--connector api|orion] [--origin <url>] [--keep-legacy]
//
// The mode is required: postgres / all copy the connector's collection into PostgreSQL. No connector: api and orion.
//
// The in-process lock does not protect a separate CLI process: run it while apiConnector is not
// polling (or between two polls), or prefer the HTTP endpoint on a running service.

process.percocologger = require("../percocologger.config")
const common = require("../utils/common")
const config = common.checkConfig(require("../config"), require("../config.template"))
require("../utils/collections").validateCollections()
const mongoose = require("mongoose")
const logger = require("percocologger")
const { runRebuild } = require("../utils/rebuild")

async function main() {
    const args = process.argv.slice(2)
    const option = name => {
        const index = args.indexOf(name)
        if (index < 0)
            return { index }
        const value = args[index + 1]
        if (!value || value.startsWith("--"))
            throw new Error(`${name} requires a value`)
        return { index, value }
    }
    const { index: originIndex, value: origin } = option("--origin")
    const { index: connectorIndex, value: connector } = option("--connector")
    const valueIndexes = [originIndex, connectorIndex].filter(i => i >= 0).map(i => i + 1)
    const mode = args.find((a, i) => !a.startsWith("--") && !valueIndexes.includes(i))
    if (!mode)
        throw new Error("Usage: node scripts/rebuildFromSources.js <entries|postgres|all> [--connector api|orion] [--origin <url>] [--keep-legacy]")
    const keepLegacy = args.includes("--keep-legacy")

    await mongoose.connect(config.mongo)
    try {
        console.log(JSON.stringify(await runRebuild({ mode, connector, origin, keepLegacy }), null, 2))
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
