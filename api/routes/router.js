const express = require("express")
const controller = require("../controllers/controller.js")
const router = express.Router()
const { auth } = require("../middlewares/auth.js")

// Orion notifications: open, like a webhook. Orion cannot send a Keycloak token (it would expire inside the
// subscription), and the recovery check (utils/common.verifyLostSubscriptionOrion) posts here too. Without mapID in
// the url: the entity's mapID, else config.mapID.
router.post(encodeURI("/orion/subscribe/:mapID"), controller.notifyPath)
router.post(encodeURI("/orion/subscribe"), controller.notifyPath)
router.put(encodeURI("/query"), auth, controller.sync)
// What anyone may see of the configuration (read only, nothing secret): no authentication
router.get(encodeURI("/config"), controller.publicConfig)
router.get(encodeURI("/queue"), auth, controller.queue)
router.post(encodeURI("/rebuild"), auth, controller.rebuild)
router.get(encodeURI("/rebuild"), auth, controller.rebuildStatus)
router.post(encodeURI("/incremental/reset"), auth, controller.resetIncremental)
router.get(encodeURI("/incremental/reset"), auth, controller.resetIncrementalStatus)
router.get(encodeURI("/incremental/reset/log"), auth, controller.resetIncrementalLog)
// Backups of the apiConnector data (utils/apiBackups.js): list and log for the logged users; backup, restore and
// delete with the management password too
router.get(encodeURI("/backups"), auth, controller.listBackups)
router.get(encodeURI("/backups/log"), auth, controller.backupLog)
router.post(encodeURI("/backups"), auth, controller.createBackup)
router.post(encodeURI("/backups/:id/restore"), auth, controller.restoreBackup)
router.delete(encodeURI("/backups/:id"), auth, controller.deleteBackup)

module.exports = router
