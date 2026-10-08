// One-off: sets the format "object" on the Orion refs of keys / values / entries (see utils/formatsMigration.js),
// instead of rebuilding the Orion entries. For the API refs: POST /api/rebuild?mode=entries&connector=api.
//
//   node scripts/migrateOrionFormats.js --dry-run         what would change (and the Orion origins left out)
//   node scripts/migrateOrionFormats.js                   sets them
//   node scripts/migrateOrionFormats.js --all-object      without checking the Orion origins that are not datapoints
//                                                        (one record each): all "object"
//
// The datapoints are not read: their origins come from the keys `survey` / `dimensions`.
//
// Re-runnable: the refs that already have a format are not touched.

process.percocologger = require("../percocologger.config")
const common = require("../utils/common")
const config = common.checkConfig(require("../config"), require("../config.template"))
require("../utils/collections").validateCollections()
const mongoose = require("mongoose")
const logger = require("percocologger")
const { migrateOrionFormats } = require("../utils/formatsMigration")

async function main() {
    const args = process.argv.slice(2)
    const unknown = args.filter(a => !["--dry-run", "--all-object"].includes(a))
    if (unknown.length)
        throw new Error("Unknown arguments: " + unknown.join(" ") + " (--dry-run | --all-object)")
    await mongoose.connect(config.mongo)
    try {
        const stats = await migrateOrionFormats({ dryRun: args.includes("--dry-run"), checkRecords: !args.includes("--all-object") })
        console.log(JSON.stringify(stats, null, 2))
        const left = Object.keys(stats.originsLeftOut)
        if (left.length)
            console.log("Orion origins left out (records that are not plain objects): rebuild each one with\n" +
                left.map(o => `  POST /api/rebuild?mode=entries&connector=orion&origin=${encodeURIComponent(o)}`).join("\n"))
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
