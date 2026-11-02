const service = require("../services/service.js")
const logger = require('percocologger')
const rebuild = require("../../utils/rebuild")
const resetIncremental = require("../../utils/resetIncremental")
const config = require("../../config")
const locks = require("../../utils/jobLocks")

module.exports = {



    sync: async (req, res) => {
        logger.info("Sync")
        return await res.send(await service.sync())
    },

    notifyPath: async (req, res) => {
        logger.info("Notification received")
        try {
            let response = await service.notifyPath(req, res)
            if (typeof response == "string")
                return res.send(response)
            else
                res.status(500).send(response?.toString() == "[object Object]" ? response : response.toString())
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    queue : async (req, res) => {
        logger.info("Queue")
        return await res.send({ queue: await service.queue() })
    },

    // POST /rebuild?mode=entries|postgres|all&connector=api|orion&origin=<url>&keepLegacy=true (also accepted in the
    // JSON body). mode is required: postgres / all copy the connector's collection into PostgreSQL, never by default.
    // No connector: api and orion; origin requires a connector. Starts the rebuild in background and answers 202;
    // progress/result via GET /rebuild.
    rebuild: async (req, res) => {
        const pick = name => req.query?.[name] ?? req.body?.[name]
        const params = {
            mode: pick("mode") || undefined,
            connector: pick("connector") || undefined,
            origin: pick("origin") || undefined,
            keepLegacy: pick("keepLegacy") === true || pick("keepLegacy") === "true"
        }
        const invalid = rebuild.validate(params)
        if (invalid)
            return res.status(400).send(invalid)
        if (locks.rebuilding)
            return res.status(409).send("A rebuild is already running")
        if (locks.polling)
            return res.status(409).send("An API poll is running, retry when it has finished")
        logger.info("Rebuild from sources requested", params)
        rebuild.runRebuild(params).catch(() => { }) // outcome is stored in the status (GET /rebuild)
        res.status(202).send({ started: true, ...params })
    },

    rebuildStatus: async (req, res) => {
        res.send(rebuild.getRebuildStatus())
    },

    // POST /incremental/reset?name=<api name>&name=... (also { names: [...] } in the JSON body; none: every incremental
    // API), &dryRun=true: only the counts. Deletes the records of those APIs and polls them again from the start
    // (utils/resetIncremental.js), in background: 202; progress/result via GET /incremental/reset.
    resetIncremental: async (req, res) => {
        const pick = name => req.query?.[name] ?? req.body?.[name]
        const raw = pick("name") ?? pick("names")
        const names = (Array.isArray(raw) ? raw : raw === undefined ? [] : [raw]).map(String).filter(Boolean)
        const dryRun = pick("dryRun") === true || pick("dryRun") === "true"
        let apis
        try {
            apis = resetIncremental.selectApis(names)
        }
        catch (error) {
            return res.status(400).send(error.message)
        }
        if (dryRun)
            return res.send(await resetIncremental.resetIncremental({ names, dryRun: true }))
        if (!config.sourceConnectors?.apiConnector)
            return res.status(409).send("apiConnector is disabled (sourceConnectors.apiConnector): nothing would poll the APIs again")
        if (locks.rebuilding)
            return res.status(409).send("A rebuild or a reset is already running")
        if (locks.polling)
            return res.status(409).send("An API poll is running, retry when it has finished")
        const { pollAPI } = require("../../inputConnectors/apiConnector") // already loaded by index.js
        logger.info("Reset of the incremental APIs requested", apis.map(api => api.name))
        resetIncremental.resetIncremental({ names, pollAPI }).catch(() => { }) // outcome in the status
        res.status(202).send({ started: true, apis: apis.map(api => api.name) })
    },

    resetIncrementalStatus: async (req, res) => {
        res.send(resetIncremental.getResetStatus())
    }

}