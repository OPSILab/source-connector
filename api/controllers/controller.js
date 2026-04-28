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
        return await res.send(await service.queue())
    }

}