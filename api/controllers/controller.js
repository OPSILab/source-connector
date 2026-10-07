const service = require("../services/service.js")
const logger = require('percocologger')
const rebuild = require("../../utils/rebuild")
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
    }

}