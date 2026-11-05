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
router.get(encodeURI("/queue"), auth, controller.queue)
router.post(encodeURI("/rebuild"), auth, controller.rebuild)
router.get(encodeURI("/rebuild"), auth, controller.rebuildStatus)
router.post(encodeURI("/incremental/reset"), auth, controller.resetIncremental)
router.get(encodeURI("/incremental/reset"), auth, controller.resetIncrementalStatus)
router.get(encodeURI("/incremental/reset/log"), auth, controller.resetIncrementalLog)

module.exports = router
