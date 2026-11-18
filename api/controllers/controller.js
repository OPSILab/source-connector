const service = require("../services/service.js")
const logger = require('percocologger')
const rebuild = require("../../utils/rebuild")
const resetIncremental = require("../../utils/resetIncremental")
const config = require("../../config")
const locks = require("../../utils/jobLocks")
const managementSecret = require("../../utils/managementSecret")
const publicConfig = require("../../utils/publicConfig")
const apiBackups = require("../../utils/apiBackups")

// The HTTP status of an error of utils/apiBackups.js
const backupErrorStatus = error => ({ BUSY: 409, NOT_FOUND: 404, INVALID: 400 })[error?.code] || 500

// The checks of a backup action before it starts: the management password, apiConnector enabled, nothing running
function backupRefusal(req) {
    const refused = managementSecret.refusal(req)
    if (refused)
        return [403, refused]
    if (locks.rebuilding)
        return [409, "A rebuild, a reset or a backup is already running"]
    if (locks.polling)
        return [409, "An API poll is running, retry when it has finished"]
}

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

    // GET /config (no authentication): the connectors enabled and the APIs polled, without anything secret
    // (utils/publicConfig.js)
    publicConfig: async (req, res) => {
        res.send(publicConfig.publicConfig())
    },

    // GET /queue: the Orion notifications waiting or being processed. { queue: their number, processing: the one being
    // processed ({ receivedAt, startedAt, entities: [{ id, url }] }) or null, waiting: the others, in arrival order }
    queue : async (req, res) => {
        logger.info("Queue")
        const jobs = service.queueJobs ? service.queueJobs() : []
        return await res.send({
            queue: await service.queue(),
            processing: jobs.find(job => job.startedAt) || null,
            waiting: jobs.filter(job => !job.startedAt)
        })
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

    // POST /incremental/reset?name=<api name>&name=... (also { names: [...] } in the JSON body): any API, incremental or
    // not; none: every incremental API. &dryRun=true: only the counts (none named: every API, with `incremental`).
    // Deletes the records of those APIs and polls them again from the start
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
            apis = resetIncremental.selectApis(names, { everyIfNone: dryRun })
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
        logger.info("Reset of the APIs requested", apis.map(api => api.name))
        resetIncremental.resetIncremental({ names, pollAPI, by: managementSecret.requester(req) }).catch(() => { }) // outcome in the status (and the log)
        res.status(202).send({ started: true, apis: apis.map(api => api.name) })
    },

    resetIncrementalStatus: async (req, res) => {
        res.send(resetIncremental.getResetStatus())
    },

    // GET /backups: the backups of the apiConnector data (newest first), the action running or the last one, the
    // limit of backups kept and whether the management password is configured (backup / restore / delete need it)
    listBackups: async (req, res) => {
        try {
            res.send({ items: await apiBackups.listBackups(), status: apiBackups.getBackupStatus(), maxBackups: apiBackups.maxBackups(), secretConfigured: managementSecret.configured() })
        }
        catch (error) {
            logger.error(error)
            res.status(backupErrorStatus(error)).send(error.message || "The backups could not be read")
        }
    },

    // GET /backups/log?limit=: every backup, restore and delete, newest first (default the latest 100)
    backupLog: async (req, res) => {
        const limit = req.query?.limit === undefined ? 100 : Number(req.query.limit)
        if (!Number.isInteger(limit) || limit < 1 || limit > 1000)
            return res.status(400).send("limit must be 1-1000")
        try {
            res.send({ items: await apiBackups.listLog({ limit }) })
        }
        catch (error) {
            logger.error(error)
            res.status(500).send("The log of the backups could not be read")
        }
    },

    // POST /backups { label? }: a backup of the apiConnector data, in background: 202; progress via GET /backups
    createBackup: async (req, res) => {
        const refused = backupRefusal(req)
        if (refused)
            return res.status(refused[0]).send(refused[1])
        const label = typeof req.body?.label == "string" ? req.body.label.trim() : undefined
        apiBackups.createBackup({ label: label || undefined, by: managementSecret.requester(req) }).catch(() => { }) // outcome in the status and the log
        res.status(202).send({ started: true })
    },

    // POST /backups/:id/restore: a backup of the data as they are now, then backup :id in their place (keys / values /
    // entries and the PostgreSQL mirror rebuilt), in background: 202
    restoreBackup: async (req, res) => {
        const refused = backupRefusal(req)
        if (refused)
            return res.status(refused[0]).send(refused[1])
        try {
            await apiBackups.listBackups().then(items => {
                if (!items.some(item => item.id === req.params.id))
                    throw Object.assign(new Error(`No API backup ${req.params.id}`), { code: "NOT_FOUND" })
            })
        }
        catch (error) {
            return res.status(backupErrorStatus(error)).send(error.message)
        }
        apiBackups.restoreBackup({ id: req.params.id, by: managementSecret.requester(req) }).catch(() => { })
        res.status(202).send({ started: true, id: req.params.id })
    },

    // DELETE /backups/:id
    deleteBackup: async (req, res) => {
        const refused = backupRefusal(req)
        if (refused)
            return res.status(refused[0]).send(refused[1])
        try {
            res.send(await apiBackups.deleteBackup({ id: req.params.id, by: managementSecret.requester(req) }))
        }
        catch (error) {
            res.status(backupErrorStatus(error)).send(error.message)
        }
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