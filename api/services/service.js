const logger = require("percocologger");
const log = logger.info;
const Datapoints = require("../models/Datapoint");
const Dimensions = require("../models/Dimensions");
const config = require("../../config");
const minioWriter = require("../../inputConnectors/minioConnector");
const axios = require("axios");
const fs = require("fs");
const { updateJWT } = require("../../utils/keycloak");
let bearerToken;
const Entity = require("../models/Entity")
const { sleep, verifyLostSubscription, checkMustUpdateDistributionDcatAp, fastAndPartialOrionizeEntity } = require("../../utils/common")
updateJWT()
  .then((token) => {
    logger.debug("Obtained Keycloak token:", token);
    bearerToken = token;
    logger.info("Initial Keycloak token obtained");
  })
  .catch((error) => logger.error(error.response?.data || error));

const path = require("path");
let attrWithUrl = config.orion?.attrWithUrl || "datasetUrl";
require("../../inputConnectors/orionConnector");

function extractValue(ent, attr, nestedAttr, defaultValue) {
  if (nestedAttr)
    return ent[attr] ?
      ent[attr].value ?
        ent[attr].value["@value"] :
        ent[attr]["@value"] :
      defaultValue;
  return ent[attr] ?
    ent[attr].value ?
      ent[attr].value :
      ent[attr] :
    defaultValue;

}

function extractDownloadURL(ent) {
  let downloadURL
  if (
    ent[attrWithUrl] &&
    typeof ent[attrWithUrl] === "object" &&
    "value" in ent[attrWithUrl]
  ) {
    downloadURL = ent[attrWithUrl].value || ent[attrWithUrl];
  } else if (ent[attrWithUrl]) {
    downloadURL = ent[attrWithUrl];
  } else if (ent[attrWithUrl + ":value"]) {
    downloadURL = ent[attrWithUrl + ":value"];
  } else if (ent.value) {
    downloadURL = ent.value;
  }
  return downloadURL
}

let requestStack = []
async function executeRequest(req, res) {
  logger.info({ body: JSON.stringify(req.body) });

  const data = req.body.data || req.body.value || req.body;
  const entities = Array.isArray(data) ? data : [data];

  for (const ent of entities) {
    const id = ent.id || ent["@id"]
    const modifiedDate = extractValue(ent, "modifiedDate", "@value", "unknown-date");
    let downloadURL = extractValue(ent, attrWithUrl)
    const format = extractValue(ent, "format")
    if (format && format.toLowerCase() != "xml") {
      logger.warn(`Entity ${id} has format ${format}, skipping...`);
      continue
    }
    if (!downloadURL) {
      downloadURL = extractDownloadURL(ent)
      if (!downloadURL || typeof downloadURL !== "string") {
        logger.warn(`no URL found for entity ${id}`);
        continue;
      }
    }
    logger.info(`Processing entity ${id} with modifiedDate ${modifiedDate}`);
    let existingEntity = id ?
      await Entity.findOne({ entityId: id }) :
      await Entity.findOne(ent)
    if (!existingEntity)
      existingEntity = await Entity.findOne({ [`${attrWithUrl}.value`]: downloadURL })
    if (!existingEntity)
      existingEntity = await Entity.findOne({ [attrWithUrl]: downloadURL })
    logger.info(`Existing entity: ${existingEntity}`);
    let mustUpdate, mustDownload
    if (existingEntity)
      mustUpdate = checkMustUpdateDistributionDcatAp(fastAndPartialOrionizeEntity(ent), fastAndPartialOrionizeEntity(existingEntity))
    else {
      mustDownload = true
      logger.info(`Entity ${id} not found in database, mapping...`)
    }
    if (!mustDownload && !mustUpdate) {
      logger.info(`Entity ${id} is up to date, skipping...`)
      continue
    }
    let mapID =
      req.query.mapID || req.params.mapID || ent.mapID || config.mapID;

    let retry = 2;
    let correctlyInserted = false
    if (!mapID) {
      const response = await axios.get(downloadURL);
      if (response?.data?.data?.datapoints)
        await Datapoints.insertMany(response.data.data.datapoints);
      else
        await minioWriter.insertInDBs(response.data, {
          name: id + "-" + path.basename(new URL(downloadURL).pathname),
          lastModified: new Date(),
          versionId: "null",
          isDeleteMarker: false,
          bucketName: "orion-notify",
          size: response.data.length,
          isLatest: true,
          etag: "",
          insertedBy: "orion-notify",
        });
    }
    else {
      let response
      while (retry > 0) {
        try {
          let map
          let maxWaitingTime = 100000; // Maximum waiting time in milliseconds
          while(!bearerToken) {
            logger.info("Waiting for bearer token to be available...");
            await sleep(1000);
            maxWaitingTime -= 1000;
            if (maxWaitingTime <= 0) {
              throw new Error("Bearer token not available after waiting for 100 seconds.");
            }
          }
          try {
            logger.debug("Fetching map from API Connector for downloadURL:", downloadURL);
            map = await axios.get(config.getMapEndpoint || "http://localhost:5500/api/map", {
              params: {
                description: downloadURL
              },
              headers: {
                Authorization: `Bearer ${bearerToken}`,
              }
            }
            )
          }
          catch (error) {
            logger.debug("Error fetching map from API Connector:", error.response?.data || error.message);
            if (error.response?.status == "404" || error.response?.status == 404)
              logger.warn("No map. Parsing instead")
            else 
              logger.error("Error fetching map from API Connector:", error.response?.data || error.message);
          }
          if (map?.data)
            response = await axios.post(
              config.mapEndpoint,
              {
                sourceDataType: format.toLowerCase() === "xml" ? "sdmx-xml" : format.toLowerCase(),
                sourceDataURL: downloadURL,
                decodeOptions: {
                  decodeFrom: format.toLowerCase() === "xml" ? "sdmx-xml" : format.toLowerCase(),
                },
                config: {
                  NGSI_entity: false,
                  ignoreValidation: true,
                  writers: [],
                  disableAjv: true,
                  mappingReport: true,
                  newSdmxDecode : true//,
                  //mappingMode: "light"
                },
                mapDescription: downloadURL
              },
              {
                headers: {
                  Authorization: `Bearer ${bearerToken}`,
                },
              }
            );
          else
            response = await axios.post(
              config.parseEndpoint,
              //config.mapEndpoint,
              {
                sourceDataType: format.toLowerCase() === "xml" ? "sdmx-xml" : format.toLowerCase(),
                sourceDataURL: downloadURL,
                decodeOptions: {
                  decodeFrom: format.toLowerCase() === "xml" ? "sdmx-xml" : format.toLowerCase(),
                },
                config: {
                  NGSI_entity: false,
                  ignoreValidation: true,
                  writers: [],
                  disableAjv: true,
                  mappingReport: true,
                  newSdmxDecode : false
                },
              },
              {
                headers: {
                  Authorization: `Bearer ${bearerToken}`,
                },
              }
            );
          retry -= 2;
          try {
            logger.info("Inserting datapoints into DB...");
            logger.info(response.data);
            let outputId = response.data[response.data.length - 1].MAPPING_REPORT.outputId
            let lastId
            let purged = false
            for (let chunkIndex = 0; (response.data[0] || response.data.id); chunkIndex++) {
              //while (response.data[0] || response.data.id) {
              logger.info(response.data?.status || response.status || response.statusCode || response.data || response);
              logger.info(`Fetching chunk ${chunkIndex} for outputId ${outputId}`);
              response = await axios.get((config.sessionEndpoint || "http://localhost:5500/api/output?") + "id=" + outputId + "&lastId=" + lastId + "&index=" + chunkIndex, {
                headers: {
                  Authorization: `Bearer ${bearerToken}`
                }
              })
              if (response.data[0]) {
                if (!purged && !config.upsertRecords)
                  await Datapoints.deleteMany({ survey: response.data[0].survey });
                const dataToInsert = response.data.map((d) => {
                  return {
                    ...d,
                    fromUrl: downloadURL,
                  };
                });
                if (config.upsertRecords)
                  await Datapoints.upsertMany(dataToInsert); //.map(d => {return {...d, dimensions : {...(d.dimensions), year : d.dimensions.time}}})) //TODO check if datapoints or other data and generalize insertion
                else
                  await Datapoints.insertMany(dataToInsert)
                lastId = response.data[response.data.length - 1]?._id
                purged = true;
                const surveyKey = dataToInsert[0].survey.toUpperCase().replace(/\./g, "");
                const dimensionsFound = await Dimensions.findOne({ survey: surveyKey });
                const uniqueKeys = new Set();
                for (const obj of dataToInsert) {
                  for (const key in obj.dimensions) {
                    uniqueKeys.add(key);
                  }
                }
                if (dimensionsFound) {
                  for (const key in dimensionsFound.dimensions) {
                    uniqueKeys.add(key);
                  }
                }
                let dimensionsObject = {}
                for (let key of Array.from(uniqueKeys))
                  dimensionsObject[key] = true
                const dimensionObject = {
                  dimensions: dimensionsObject,
                  survey: surveyKey
                };
                await Dimensions.findOneAndUpdate(
                  { survey: surveyKey },
                  dimensionObject,
                  { upsert: true, new: true }
                );
                logger.debug("Dimension object saved/updated:", dimensionObject);
              }
              else if (chunkIndex === 0)
                logger.warn("No datapoints found in the first chunk, skipping insertion.");
            }
            correctlyInserted = true;
          } catch (error) {
            logger.error("Error inserting datapoints:", error);
          }
        } catch (error) {
          logger.error(
            "Error fetching mapped data from API Connector:",
            error.response?.data || error.message
          );
          if (error.response?.status == "403" || error.response?.status == 403)
            try {
              bearerToken = await updateJWT(true);
              retry--;
            } catch (e) {
              logger.error("Error updating JWT:", e);
              retry--;
            }
          else {
            retry -= 2
            break
          }
        }
      }
      //logger.info(response.data.length)

      /*for (let i in response.data)
                  await minioWriter.insertInDBs(response.data[i], {
                      name: response.data[i].id || mapID + '-' + path.basename((new URL(downloadURL)).pathname) + i,
                      lastModified: new Date(),
                      versionId: 'null',
                      isDeleteMarker: false,
                      bucketName: 'orion-notify',
                      size: response.data.length,
                      isLatest: true,
                      etag: '',
                      insertedBy: 'orion-notify'
                  });*/
    }
    logger.info(`downloaded ${downloadURL}`);
    if (correctlyInserted)
      if (mustUpdate)
        await Entity.findOneAndUpdate({ _id: existingEntity._id }, { ...ent, entityId: id })
      else if (mustDownload)
        await Entity.insertMany([{ ...ent, entityId: id }])
  }
  return "OK";
}

module.exports = {

  queue() {
    return requestStack.length
  },

  notifyPath: async (req, res) => {
    let turn = requestStack.length
    let result
    requestStack.push([req, res])
    while (turn && requestStack.length > turn)
      await sleep(100)
    try {
      result = await executeRequest(...requestStack[0])
    }
    catch (error) {
      logger.error(error)
      return error
    }
    requestStack.shift()
    return result
  },

  sync() {
    if (config.sourceConnectors.minioConnector)
      minioWriter.sync()
    if (config.sourceConnectors.orionConnector)
      verifyLostSubscription()
  },
};
