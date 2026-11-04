const service = require("../services/service.js")
const logger = require('percocologger')
const rebuild = require("../../utils/rebuild")
const resetIncremental = require("../../utils/resetIncremental")
const config = require("../../config")
const locks = require("../../utils/jobLocks")
const managementSecret = require("../../utils/managementSecret")

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
    // (utils/resetIncremental.js), in background: 202; progress/result via GET /incremental/reset, every reset in
    // GET /incremental/reset/log. Besides the login, the management password (X-Management-Secret header, config
    // managementSecret; the dry run does not need it): 403 without it.
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
        const refused = managementSecret.refusal(req)
        if (refused)
            return res.status(403).send(refused)
        if (!config.sourceConnectors?.apiConnector)
            return res.status(409).send("apiConnector is disabled (sourceConnectors.apiConnector): nothing would poll the APIs again")
        if (locks.rebuilding)
            return res.status(409).send("A rebuild or a reset is already running")
        if (locks.polling)
            return res.status(409).send("An API poll is running, retry when it has finished")
        const { pollAPI } = require("../../inputConnectors/apiConnector") // already loaded by index.js
        logger.info("Reset of the incremental APIs requested", apis.map(api => api.name))
        resetIncremental.resetIncremental({ names, pollAPI, by: managementSecret.requester(req) }).catch(() => { }) // outcome in the status (and the log)
        res.status(202).send({ started: true, apis: apis.map(api => api.name) })
    },

    resetIncrementalStatus: async (req, res) => {
        res.send(resetIncremental.getResetStatus())
    },

    // GET /incremental/reset/log?limit=: the resets done, newest first (default the latest 100), and whether the
    // management password is configured (the reset is disabled without it)
    resetIncrementalLog: async (req, res) => {
        const limit = req.query?.limit === undefined ? 100 : Number(req.query.limit)
        if (!Number.isInteger(limit) || limit < 1 || limit > 1000)
            return res.status(400).send("limit must be 1-1000")
        try {
            res.send({ items: await resetIncremental.listResets({ limit }), secretConfigured: managementSecret.configured() })
        }
        catch (error) {
            logger.error(error)
            res.status(500).send("The log of the resets could not be read")
        }
    }

}