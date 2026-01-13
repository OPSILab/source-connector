const logger = require('percocologger')
const log = logger.info

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