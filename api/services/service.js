const logger = require("percocologger");
const log = logger.info;
const Dimensions = require("../models/Dimensions");
const config = require("../../config");
const minioWriter = require("../../inputConnectors/minioConnector");
const axios = require("axios");
const fs = require("fs");
const { updateJWT } = require("../../utils/keycloak");
let bearerToken;
const Entity = require("../models/Entity")
const { replaceRecords } = require("../../utils/sourceRecords")
const { createQueue } = require("../../utils/requestQueue")
// Orion data goes to the Orion collection (collections.orion): datapoints upserted by dupl_hash (utils/datapoints.js),
// other records replaced by dataset url (utils/sourceRecords.js)
const { upsertDatapoints, openDatapointsWriter } = require("../../utils/datapoints")
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

const sessionEndpoint = () => config.sessionEndpoint || "http://localhost:5500/api/output?"
const reportEndpoint = () => config.reportEndpoint || "http://localhost:5500/api/reportSync"
const feedbackEndpoint = () => config.feedbackEndpoint || "http://localhost:5500/api/feedback"

function withQuery(base, params) {
  const query = Object.entries(params).map(([key, value]) => key + "=" + encodeURIComponent(value)).join("&")
  if (!base.includes("?"))
    return base + "?" + query
  return base + (base.endsWith("?") || base.endsWith("&") ? "" : "&") + query
}

// GET towards the Data Model Mapper; on 403 the token is refreshed once (a long mapping outlives it)
async function dmmGet(url) {
  try {
    return await axios.get(url, { headers: { Authorization: `Bearer ${bearerToken}` } })
  } catch (error) {
    if (error.response?.status != 403)
      throw error
    logger.info("Token refused by the Data Model Mapper, refreshing it")
    bearerToken = await updateJWT(true)
    return await axios.get(url, { headers: { Authorization: `Bearer ${bearerToken}` } })
  }
}

async function sendFeedback(outputId) {
  for (let attempt = 1; ; attempt++)
    try {
      return await dmmGet(withQuery(feedbackEndpoint(), { id: outputId, deleteAfterRead : "true" }))
    } catch (error) {
      if (attempt >= 5)
        throw new Error(`Feedback for session ${outputId} not delivered: ${error.response?.data || error.message}`)
      logger.warn(`Feedback for session ${outputId} failed (attempt ${attempt}), retrying:`, error.response?.data || error.message)
      await sleep(1000 * attempt)
    }
}

async function updateDimensions(dataToInsert) {
  const surveyKey = dataToInsert[0].survey.toUpperCase().replace(/\./g, "");
  const dimensionsFound = await Dimensions.findOne({ survey: surveyKey });
  const uniqueKeys = new Set();
  for (const obj of dataToInsert)
    for (const key in obj.dimensions)
      uniqueKeys.add(key);
  if (dimensionsFound)
    for (const key in dimensionsFound.dimensions)
      uniqueKeys.add(key);
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

// Reads a streamed mapping session (POST .../transform?streamMode=true answered { id }) part by part:
// GET /output?id&index -> parts in order; an empty answer means "not written yet" or "no more parts", told apart
// with GET /reportSync (completed: true once the session has ended). Each stored part is acknowledged with
// GET /feedback?id, which lets the mapper write the next one (config.waitForFeedback).
// Ends when the session is completed and no part is left; throws if it ended with an error, if the mapper does not
// know the session (restarted / expired) or does not answer for dmmSessionLostTimeoutMs, or if nothing moves for
// dmmNoProgressTimeoutMs.
async function readMappingSession(outputId, { downloadURL, id, sqlName }) {
  if (!outputId)
    throw new Error("The Data Model Mapper did not return a session id")
  const lostAfter = config.dmmSessionLostTimeoutMs || 5 * 60 * 1000
  const stuckAfter = config.dmmNoProgressTimeoutMs || 30 * 60 * 1000
  logger.info(`Mapping session ${outputId}: reading its output`)
  // opened at the first non-empty part: replaces the datapoints of this dataset (downloadURL)
  let writer
  let chunkIndex = 0
  let lastId
  let completed = false          // the mapper said the session has ended
  let lastAnswer = Date.now()    // last time the mapper confirmed it knows the session
  let lastProgress = Date.now()  // last time something moved (a part read, rows or parts counted by the mapper)
  let progressMark
  while (true) {
    // a fresh read at every round: the previous part must never be taken for the next one
    let chunk
    try {
      chunk = (await dmmGet(withQuery(sessionEndpoint(), { id: outputId, lastId, index: chunkIndex, deleteAfterRead : "true" }))).data
    } catch (error) {
      logger.error(`Error fetching part ${chunkIndex} of session ${outputId}:`, error.response?.data || error.message)
    }
    if (Array.isArray(chunk) && chunk.length) {
      logger.info(`Session ${outputId}: inserting ${chunk.length} datapoints of part ${chunkIndex}`)
      logger.debug("First datapoint:", chunk[0]);
      logger.debug("Last datapoint:", chunk[chunk.length - 1]);
      const dataToInsert = chunk.map((d) => ({ ...d, fromUrl: downloadURL }));
      if (!writer)
        // without upsertRecords the survey is emptied and filled again, as before
        writer = await openDatapointsWriter(downloadURL, { logName: "orion-notify " + id, sqlName, replaceSurvey: true })
      await writer.add(dataToInsert)
      lastId = chunk[chunk.length - 1]?._id
      await updateDimensions(dataToInsert)
      await sendFeedback(outputId)
      chunkIndex++
      lastAnswer = lastProgress = Date.now()
      continue
    }
    if (completed)
      break // ended, and still nothing at the last look: all parts have been read
    let status
    try {
      status = (await dmmGet(withQuery(reportEndpoint(), { id: outputId, deleteAfterRead : "true" }))).data
    } catch (error) {
      logger.error(`Error fetching the state of session ${outputId}:`, error.response?.data || error.message)
    }
    if (status?.completed) {
      if (status.status === "error")
        throw new Error(`Mapping session ${outputId} ended with an error: ${status.error}`)
      logger.info(`Mapping session ${outputId} completed, ${status.parts} parts:`, status.MAPPING_REPORT)
      if (status.parts !== undefined && status.parts > chunkIndex)
        logger.debug(`Session ${outputId}: ${status.parts - chunkIndex} parts still to read`)
      completed = true
      continue // one more look: the last part may have been written between the two calls
    }
    const now = Date.now()
    if (status?.status) { // known and alive (starting / running)
      lastAnswer = now
      const mark = status.parts + ":" + status.MAPPING_REPORT?.Processed_objects
      if (mark !== progressMark) {
        progressMark = mark
        lastProgress = now
        logger.info(`Session ${outputId} ${status.status}:`, status.MAPPING_REPORT)
      }
    }
    if (now - lastAnswer > lostAfter)
      throw new Error(`Mapping session ${outputId} lost: ${status?.data || "the mapper does not answer"} for ${lostAfter / 1000}s`)
    if (now - lastProgress > stuckAfter)
      throw new Error(`Mapping session ${outputId} stuck: no progress for ${stuckAfter / 1000}s`)
    await sleep(1000)
  }
  if (writer)
    await writer.close()
  else
    logger.warn(`Session ${outputId}: no datapoints in the output`)
  return chunkIndex
}

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
    const sqlName = id + "-" + path.basename(new URL(downloadURL).pathname)
    if (!mapID) {
      const response = await axios.get(downloadURL);
      if (response?.data?.data?.datapoints)
        await upsertDatapoints(response.data.data.datapoints, downloadURL, { logName: "orion-notify " + id, sqlName });
      else
        // the dataset as it is: Orion records, replaced by dataset url
        await replaceRecords(response.data, downloadURL, {
          logName: "orion-notify " + id,
          sqlName,
          connector: "orion"
        });
      correctlyInserted = true // reached only if the insertion above did not throw
    }
    else {
      let response
      while (retry > 0) {
        try {
          let map
          let maxWaitingTime = 100000; // Maximum waiting time in milliseconds
          while (!bearerToken) {
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
                  newSdmxDecode: true,
                  waitForFeedback: true // the mapper waits for GET /feedback after each part (see readMappingSession)
                  //,
                  //mappingMode: "light"
                },
                mapDescription: downloadURL
              },
              {
                headers: {
                  Authorization: `Bearer ${bearerToken}`,
                },
                params: {
                  streamMode: true
                }
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
                  newSdmxDecode: false,
                  waitForFeedback: true
                },
              },
              {
                headers: {
                  Authorization: `Bearer ${bearerToken}`,
                },
                params: {
                  streamMode: true
                }
              }
            );
          retry -= 2;
          try {
            await readMappingSession(response.data?.id, { downloadURL, id, sqlName })
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

const notifications = createQueue(executeRequest)

module.exports = {

  // notifications waiting or being processed (GET /api/queue)
  queue() {
    return notifications.size()
  },

  // One notification at a time, in arrival order: an error ends that notification only (it is returned, as before),
  // the next ones go on - see utils/requestQueue.js
  notifyPath: async (req, res) => {
    try {
      return await notifications.push(req, res)
    }
    catch (error) {
      logger.error(error)
      return error
    }
  },

  sync() {
    if (config.sourceConnectors.minioConnector)
      minioWriter.sync()
    if (config.sourceConnectors.orionConnector)
      verifyLostSubscription()
  },
};
