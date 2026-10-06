// CLI wrapper of utils/rebuild.js (same logic as POST /api/rebuild).
//
//   node scripts/rebuildFromSources.js [entries|postgres|all] [--origin <url>] [--keep-legacy]
//
// The in-process lock does not protect a separate CLI process: run it while apiConnector is not
// polling (or between two polls), or prefer the HTTP endpoint on a running service.

process.percocologger = require("../percocologger.config")
const common = require("../utils/common")
const config = common.checkConfig(require("../config"), require("../config.template"))
const mongoose = require("mongoose")
const logger = require("percocologger")
const { runRebuild } = require("../utils/rebuild")

async function main() {
    const args = process.argv.slice(2)
    const originIndex = args.indexOf("--origin")
    const origin = originIndex >= 0 ? args[originIndex + 1] : undefined
    if (originIndex >= 0 && (!origin || origin.startsWith("--")))
        throw new Error("--origin requires a url")
    const mode = args.find((a, i) => !a.startsWith("--") && (originIndex < 0 || i != originIndex + 1)) || "all"
    const keepLegacy = args.includes("--keep-legacy")

    await mongoose.connect(config.mongo)
    try {
        console.log(JSON.stringify(await runRebuild({ mode, origin, keepLegacy }), null, 2))
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
