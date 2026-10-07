// One-off move to one collection per connector (see utils/collectionsMigration.js).
//
//   node scripts/migrateCollections.js --dry-run              counts only
//   node scripts/migrateCollections.js [--delete-datapoints]  moves the MinIO documents, cleans `status`, builds the
//                                                              Orion fromUrl index
//
// Then: POST /api/rebuild?mode=entries (keys / values / entries with their connector) and the MinIO sync.
// Run it with the Source-Connector stopped (or between two API polls).

process.percocologger = require("../percocologger.config")
const common = require("../utils/common")
const config = common.checkConfig(require("../config"), require("../config.template"))
require("../utils/collections").validateCollections()
const mongoose = require("mongoose")
const logger = require("percocologger")
const { migrateCollections } = require("../utils/collectionsMigration")

async function main() {
    const args = process.argv.slice(2)
    await mongoose.connect(config.mongo)
    try {
        const stats = await migrateCollections({ dryRun: args.includes("--dry-run"), deleteDatapoints: args.includes("--delete-datapoints") })
        console.log(JSON.stringify(stats, null, 2))
        if (!args.includes("--dry-run"))
            console.log("Next: POST /api/rebuild?mode=entries, then the MinIO sync (it runs at the Source-Connector start)")
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
