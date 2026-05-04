const logger = require('percocologger')
const log = logger.info
const axios = require("axios")
const config = require("../config")
const Entity = require("../api/models/Entity")

function objectCheck(objs) {
  for (let obj of objs)
    for (let key in obj)
      try {
        let valueParsed = JSON.parse(obj[key])
        obj[key] = valueParsed
      }
      catch (error) {
        logger.error(error)
      }

}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function stringify(item) {
  if (typeof item != "string")
    return JSON.stringify(item)
  return item
}

function convertCSVtoJSON(csvData) {
  logger.debug(csvData)
  const lines = csvData.split('\r\n');
  const possibleHeaders = [
    lines[0].trim().split(','),
    lines[0].trim().split(';')
  ]
  const headers = possibleHeaders[0].length > possibleHeaders[1].length ? possibleHeaders[0] : possibleHeaders[1]
  const results = [];
  for (let i = 1; i < lines.length; i++) {
    const obj = {};
    const currentLine = lines[i].trim().split(possibleHeaders[0].length > possibleHeaders[1].length ? "," : ";");
    for (let j = 0; j < headers.length; j++)
      obj[this.deleteSpaces(headers[j].replaceAll(/['"]/g, ''))] = this.deleteSpaces(currentLine[j]?.replaceAll(/['"]/g, ''));
    results.push(obj);
  }

  return JSON.stringify(results);
}

function getVisibility(name) {

  name = name.split("/")
  if (name[0].includes("@") || (name[0].toLowerCase().includes("shared data")))
    return name[0]
  return "public-data"
}

function syncEntries(obj, visibility, entries) {
  for (let key in obj)
    if (!entries[key])
      entries[key] = { [stringify(obj[key])]: [visibility] }
    else if (!entries[key][stringify(obj[key])])
      entries[key][stringify(obj[key])] = [visibility]
    else if (!entries[key][stringify(obj[key])].includes(visibility))
      entries[key][stringify(obj[key])].push(visibility)
}

module.exports = {

  async verifyLostSubscription() {
    try {
      //la seguente riga non funzionerà . Probabilmente bisogna chiamare l'ngsi broker
      //let entities = await axios.get(config.orion.orionBaseUrl + apiConnector.getEndpointVersionApi().split("subscriptions") + "/entities?type=DistributionDCAT-AP")
      let ngsiBrokerUrl = (config.orion.ngsiBrokerBaseUrl + "/api/distributiondcatap")//.replaceAll("//", "/")
      let entities = (await axios.get(ngsiBrokerUrl)).data
      logger.debug("Entities retrieved from ngsi broker: " + JSON.stringify(entities).substring(0, 100))
      for (let ent of entities) {
        const { id, type } = ent
        let orionedEnt = { id, type }
        let entCopy = JSON.parse(JSON.stringify(ent))
        delete entCopy.id
        delete entCopy.type
        for (let key in entCopy)
          entCopy[key] = { value: entCopy[key] }
        let parsedEnt = { ...orionedEnt, ...entCopy, orionId: id }
        const existingEntity = await Entity.findOne({ orionId: id })
        logger.info(ent)
        if (
          !existingEntity ||
          (
            existingEntity?.modifiedDate?.value &&
            parsedEnt?.modifiedDate?.value &&
            existingEntity.modifiedDate.value["@value"] != parsedEnt.modifiedDate.value["@value"]
          )
        )
          try {
            await axios.post("http://localhost:" + (config.port || 3001) + "/api/orion/subscribe/6914a252ddb96948ee67b2e1", {
              "id": "self",
              "type": "Notification",
              "subscriptionId": "self",
              "notifiedAt": Date.now(),
              "data": [
                parsedEnt
              ]
            })
          } catch (error) {
            logger.error(error.response?.data ? { axios: error.response.data } : error)
          }
      }
      logger.info("Lost subscription verified")
    }
    catch (error) {
      logger.error(error.response?.data ? { axios: error.response.data } : error)
    }
  },

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  },

  minify(obj) {
    try {
      if (typeof obj == "string")
        return obj.substring(0, 10).concat(" ...")
      else if (Array.isArray(obj) || typeof obj == "object")
        return JSON.stringify(obj).substring(0, 10).concat(" ...")
      return obj
    }
    catch (error) {
      logger.error(error.toString())
      return obj
    }
  },

  async getEntries(obj, type, name, entries) {// csv, jsonArray, json

    let visibility = getVisibility(name)
    if (!obj[0].csv && Array.isArray(obj[0].json) && type != "jsonArray")
      type = "jsonArray" //throw new Error("obj is a jsonArray and not " + type)
    else if ((!obj[0].csv && !Array.isArray(obj[0].json) && typeof obj == "object") && type != "json")
      //if (obj[0].features)
      type = "json" //throw new Error("obj is a json and not " + type)
    else if (obj[0].csv && type != "csv")
      type = "csv"//throw new Error("obj is a csv and not " + type)
    if (type == "json") {
      if (obj[0].features)
        obj = [{ json: obj[0].features }]
      else {
        logger.trace(obj[0])
        syncEntries(obj[0], visibility, entries)

        return
      }

      logger.trace("so it was a geojson")
    }
    logger.trace("Here's obj before flatmap")
    logger.trace(JSON.stringify(obj).substring(0, 30))
    obj = obj[0].json || obj[0].csv
    if (obj[0] && obj[0].properties)
      obj = obj.map(o => o.properties)
    for (let o of obj)
      syncEntries(o, visibility, entries)

    return
  },

  async setType(extension, jsonParsed) {
    logger.debug("csv ", extension == "csv", " array ", Array.isArray(jsonParsed), " object ", typeof jsonParsed == "object", " jsonparsed ", jsonParsed)
    return extension == "csv" ?
      "csv" :
      Array.isArray(jsonParsed) ?
        "jsonArray" :
        typeof jsonParsed == "object" ?
          "json" :
          "raw"
  },

  json2csv(obj) { //TODO : implement properly
    return JSON.stringify([obj])
  },

  parseJwt(token) {
    return JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString());
  },

  urlEncode(bucket) {
    return bucket.replaceAll("-", "")
  },

  deleteSpaces(obj) {
    if (obj) {
      while (obj[0] == " ")
        obj = obj.substring(1)
      while (obj[obj.length - 1] == " ")
        obj = obj.substring(0, obj.length - 1)
    }
    return obj
  },

  convertCSVtoJSON(csvData) {
    const lines = csvData.split('\r\n');
    const possibleHeaders = [
      lines[0].trim().split(','),
      lines[0].trim().split(';')
    ]
    const headers = possibleHeaders[0].length > possibleHeaders[1].length ? possibleHeaders[0] : possibleHeaders[1]
    const results = [];

    for (let i = 1; i < lines.length; i++) {
      const obj = {};
      const currentLine = lines[i].trim().split(possibleHeaders[0].length > possibleHeaders[1].length ? "," : ";");
      for (let j = 0; j < headers.length; j++)
        obj[this.deleteSpaces(headers[j].replaceAll(/['"]/g, ''))] = this.deleteSpaces(currentLine[j]?.replaceAll(/['"]/g, ''));
      results.push(obj);
    }

    return JSON.stringify(results);
  },

  cleaned(obj) {
    return (typeof obj != "string" ? JSON.stringify(obj) : obj).replace(/['\r\n]/g, '')
  },

  checkConfig(configIn, configTemplate) {
    //logger.info(configIn)
    for (let key in configTemplate) {
      if (typeof configIn[key] == "object")
        configIn[key] = this.checkConfig(configIn[key], configTemplate[key])
      else if (configIn[key] == undefined) {
        logger.warn(`Config key ${key} is missing, using default value`)
        configIn[key] = configTemplate[key]
      }
    }
    return configIn
  },

  bodyCheck: async (req, res, next) => {
    if (req?.body?.mongoQuery && req.body.mongoQuery[''] == '{"$gte":null,"$lte":null}')
      delete req.body.mongoQuery['']
    if (!req.body.query && req?.body?.mongoQuery && !(Object.keys(req?.body?.mongoQuery).length == 1 && req.body.mongoQuery[''] == ''))
      objectCheck([req.body.mongoQuery, req.query])
    next()
  }
}