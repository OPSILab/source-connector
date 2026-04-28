const express = require("express")
const controller = require("../controllers/controller.js")
const router = express.Router()
const { auth } = require("../middlewares/auth.js")

router.post(encodeURI("/orion/subscribe/:mapID"), auth, controller.notifyPath)
router.put(encodeURI("/query"), auth, controller.sync)
router.get(encodeURI("/queue"), auth, controller.queue)

module.exports = router
