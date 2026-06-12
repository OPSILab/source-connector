const config = require('../config')
const logger = require('percocologger')
const axios = require('axios')
const Source = require("../api/models/Models").Source
let tokens = {}

async function pollAPI() {
    try {
        const urls = config.apiConnectorConfig.apiUrls
        for (const api of urls) {
            try {
                const headers = {}
                for (const header in api.headers)
                    if (api.headers[header].type === "bearerToken") {
                        if (!api.headers[header].value || (api.headers[header].expiry && new Date() > new Date(api.headers[header].expiry))) {
                            if (tokens[api.headers[header].authUrl.value] && tokens[api.headers[header].authUrl.value].expiry && new Date() < new Date(tokens[api.headers[header].authUrl.value].expiry)) {
                                api.headers[header].value = tokens[api.headers[header].authUrl.value].token
                                if (tokens[api.headers[header].authUrl.value].expiry)
                                    api.headers[header].expiry = tokens[api.headers[header].authUrl.value].expiry
                                api.bearerPosition = header
                                //headers[header] = api.headers[header].value
                                continue
                            }
                            const response = await getToken(api.headers[header].authUrl.value, api.headers[header].authUrl.requestType, api.headers[header].credentials, api.headers[header].authProfile)
                            api.headers[header].value = "Bearer " + response.data.access_token
                            //headers[header] = api.headers[header].value
                            tokens[api.headers[header].authUrl.value] = {
                                token: api.headers[header].value,
                                expiry: response.data.expires_in ? new Date(new Date().getTime() + response.data.expires_in * 1000) : null
                            }
                            if (response.data.expires_in)
                                api.headers[header].expiry = new Date(new Date().getTime() + response.data.expires_in * 1000)
                        }
                        api.bearerPosition = header
                    }
                /*else if (api.headers[header].type === "basic" && api.headers[header].alwaysSend)
                    headers[header] = setBasicAuthHeader(api.headers[header].credentials)*/
                if (api.bearerPosition)
                    headers[api.bearerPosition] = api.headers[api.bearerPosition].value

                if (api.headers.Authorization && api.headers.Authorization.type === "basic" && api.headers.Authorization.alwaysSend)
                    headers.Authorization = setBasicAuthHeader(api.headers.Authorization.credentials)

                if (api.batch) {
                    logger.info(`Polling ${api.name} API for batch values from ${api.batch.from}...`)
                    const batchResponse = await axios.get(api.batch.from, {
                        headers
                    })
                    const batchValues = batchResponse.data.map(item => item[api.batch.param])
                    logger.info(`Batch values for ${api.name} API:`, batchValues)
                    for (const batchValue of batchValues) {
                        logger.info(`Polling ${api.name} API for batch value:`, batchValue)
                        const batchUrl = api.url.replace("{batch}", batchValue)
                        const response = await axios.get(batchUrl, {
                            headers
                        })
                        logger.info(`Data from ${api.name} API (batch ${batchValue}):`, response.data.length)
                        await Source.deleteMany({ source: batchUrl })
                        await Source.insertMany(response.data.map(item => ({ ...item, source: batchUrl })))
                    }
                }
                else {

                    const response = await axios.get(api.url, {
                        headers
                    })
                    logger.info(`Data from ${api.name} API:`, response.data.length)
                    await Source.deleteMany({ source: api.url })
                    await Source.insertMany(response.data.map(item => ({ ...item, source: api.url })))
                }
            }
            catch (error) {
                logger.error(`Error polling ${api.name} API:`)
                if (error.response) {
                    logger.error("Status:", error.response.status)
                    logger.error("Data:", error.response.data)
                    logger.error("Headers:", error.request.headers)
                    logger.error("Request:", error.request)
                    process.exit()
                }
                else
                    logger.error(error)
            }
        }
    }
    catch (error) {
        logger.error("Error polling API:", error)
    }
}

setBasicAuthHeader = (credentials) => {
    return "Basic " + Buffer.from(credentials.username + ":" + credentials.password).toString('base64');
}

async function getToken(url, requestType, credentials, authProfile) {
    if (authProfile === "basic") {
        const authorization = setBasicAuthHeader(credentials)
        const response = await axios[requestType.toLowerCase()](url, undefined, {
            headers: {
                'Authorization': authorization
            }
        });
        return response
    }
}

pollAPI()
setTimeout(pollAPI, config.apiConnectorConfig.pollInterval)