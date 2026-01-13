const service = require("../services/service.js")
const logger = require('percocologger')

module.exports = {

  

    sync: async (req, res) => {
        logger.info("Sync")
        return await res.send(await service.sync())
    },

    notifyPath: async (req, res) => {
        logger.info("Notification received")
        try {
            res.send(await service.notifyPath(req, res))
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    }
}