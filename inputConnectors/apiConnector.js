const config = require('../config')
const logger = require('percocologger')
const axios = require('axios')
const Source = require("../api/models/Models").Source
let tokens = {}

function dbHasSameData(obj1, obj2) {
    //logger.info("Comparing objects:", JSON.stringify(obj1), JSON.stringify(obj2), JSON.stringify(obj1) == JSON.stringify(obj2))
    return JSON.stringify(obj1) == JSON.stringify(obj2)
}

function makeItem(item, source) {
    let sourceId = item.id
    delete item.id
    return { ...item, source: source, sourceId: sourceId }
}

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
                            logger.info(api.headers[header], api.headers[header].authProfile)
                            const response = await getToken(api.headers[header].authUrl.value, api.headers[header].authUrl.requestType, api.headers[header].credentials, api.headers[header].authProfile)
                            logger.info("Token got", response?.data || "No response")
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

                if (api.headers?.Authorization && api.headers.Authorization.type === "basic" && api.headers.Authorization.alwaysSend)
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
                        logger.info("api ", batchUrl, "called with ", {
                            headers
                        }, { response: response?.data?.length || response.data || "no response" })
                        logger.info(`Data from ${api.name} API (batch ${batchValue}):`, response.data.length)
                        if (config.apiConnectorConfig.upsertRecords) {
                            await Source.deleteMany({ source: batchUrl })
                            await Source.insertMany(response.data.map(item => makeItem(item, batchUrl)))
                        }
                        else {
                            let existingSources = (await Source.find({ source: batchUrl }).lean())
                            existingSources.forEach(item => delete item._id)
                            const newSources = response.data.map(item => makeItem(item, batchUrl))
                            const sourcesToInsert = newSources.filter(newItem => !existingSources.some(existingItem => dbHasSameData(existingItem, newItem)))
                            if (sourcesToInsert.length > 0) {
                                await Source.insertMany(sourcesToInsert)
                                logger.info(`Inserted ${sourcesToInsert.length} new records for ${api.name} API (batch ${batchValue})`)
                            }
                            else
                                logger.info(`No new records to insert for ${api.name} API (batch ${batchValue})`)

                        }
                    }
                }
                else if (api.pagination) {
                    /*api.url = api.url
                        .replace("{offsetParam}", api.pagination.offsetParam)
                        .replace("{limitParam}", api.pagination.limitParam)
                        .replace("{offset}", api.pagination.offset || 0)
                        .replace("{limit}", api.pagination.limit)*/
                    let response
                    do {
                        let urlWithParams = api.url + (api.url.includes("?") ? "&" : "?") + `${api.pagination.limitParam}=${api.pagination.limit}&${api.pagination.offsetParam}=${api.pagination.offset}`
                        response = await axios.get(urlWithParams, { headers })
                        logger.info(`Data from ${api.name} API:`, response.data.length)
                        if (config.apiConnectorConfig.upsertRecords) {
                            await Source.deleteMany({ source: api.url })
                            await Source.insertMany(response.data.map(item => makeItem(item, api.url)))
                        }
                        else {
                            let existingSources = (await Source.find({ source: api.url }).lean())
                            existingSources.forEach(item => delete item._id)
                            const newSources = response.data.map(item => makeItem(item, api.url))
                            const sourcesToInsert = newSources.filter(newItem => !existingSources.some(existingItem => dbHasSameData(existingItem, newItem)))
                            if (sourcesToInsert.length > 0) {
                                await Source.insertMany(sourcesToInsert)
                                logger.info(`Inserted ${sourcesToInsert.length} new records for ${api.name} API`)
                            }
                            else
                                logger.info(`No new records to insert for ${api.name} API`)

                        }
                        api.pagination.offset += api.pagination.limit
                        /*api.url.split("?")[1].split("&").forEach(param => {
                            const [key, value] = param.split("=")
                            if (key === api.pagination.offsetParam)
                                api.url = api.url.replace(`${key}=${value}`, `${key}=${parseInt(value) + api.pagination.limit}`)
                        })*/

                    } while (api.pagination.condition(response));

                }
                else {

                    const response = await axios.get(api.url, {
                        headers
                    })
                    logger.info("api ", api.url, "called awith ", {
                        headers
                    }, { response: response?.data?.length || response.data || "no response" })
                    logger.info(`Data from ${api.name} API:`, response.data.length)
                    if (config.apiConnectorConfig.upsertRecords) {
                        await Source.deleteMany({ source: api.url })
                        await Source.insertMany(response.data.map(item => makeItem(item, api.url)))
                    }
                    else {
                        let existingSources = (await Source.find({ source: api.url }).lean())
                        existingSources.forEach(item => delete item._id)
                        const newSources = response.data.map(item => makeItem(item, api.url))
                        const sourcesToInsert = newSources.filter(newItem => !existingSources.some(existingItem => dbHasSameData(existingItem, newItem)))
                        if (sourcesToInsert.length > 0) {
                            await Source.insertMany(sourcesToInsert)
                            logger.info(`Inserted ${sourcesToInsert.length} new records for ${api.name} API`)
                        }
                        else
                            logger.info(`No new records to insert for ${api.name} API`)

                    }
                }
            }
            catch (error) {
                logger.error(`Error polling ${api.name} API:`)
                if (error.response) {
                    logger.error("Status:", error.response.status)
                    logger.error("Data:", error.response.data)
                    logger.error("Headers:", error.request.headers)
                    logger.error("Request:", error.request)
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
        return await axios[requestType.toLowerCase()](url, undefined, {
            headers: {
                'Authorization': authorization
            }
        });
    }
    else if (authProfile == "OAuth 2.0 Client Credentials Grant") {
        return await axios.post(url,
            "client_id=" + credentials.client_id + "&client_secret=" + credentials.client_secret + "&grant_type=client_credentials",
            {
                headers: {
                    "Content-Type": "application/x-www-form-urlencoded"
                }
            }
        );
    }
    else
        throw new Error("No auth profile detected")
}

pollAPI()
setInterval(pollAPI, config.apiConnectorConfig.pollInterval)