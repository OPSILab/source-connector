const logger = require("percocologger");
const log = logger.info;
const Datapoints = require("../models/Datapoint");
const config = require("../../config");
const minioWriter = require("../../inputConnectors/minioConnector");
const axios = require("axios");
const fs = require("fs");
const { updateJWT } = require("../../utils/keycloak");
let bearerToken;
updateJWT()
  .then((token) => {
    bearerToken = token;
    logger.info("Initial Keycloak token obtained");
  })
  .catch((error) => logger.error(error.response?.data || error));

const path = require("path");
let attrWithUrl = config.orion?.attrWithUrl || "datasetUrl";
require("../../inputConnectors/apiConnector");

module.exports = {
  notifyPath: async (req, res) => {
    logger.info({ body: JSON.stringify(req.body) });

    const data = req.body.data || req.body.value || req.body;
    const entities = Array.isArray(data) ? data : [data];

    for (const ent of entities) {
      const id = ent.id || ent["@id"] || "unknown-id";
      let urlValue;
      if (
        ent[attrWithUrl] &&
        typeof ent[attrWithUrl] === "object" &&
        "value" in ent[attrWithUrl]
      ) {
        urlValue = ent[attrWithUrl].value;
      } else if (ent[attrWithUrl]) {
        urlValue = ent[attrWithUrl];
      } else if (ent[attrWithUrl + ":value"]) {
        urlValue = ent[attrWithUrl + ":value"];
      } else if (ent.value) {
        urlValue = ent.value;
      }

      if (!urlValue || typeof urlValue !== "string") {
        console.warn(`no URL found for entity ${id}`);
        continue;
      }

      let mapID =
        req.query.mapID || req.params.mapID || ent.mapID || config.mapID;

      if (!mapID) {
        const response = await axios.get(urlValue);
        if (response?.data?.data?.datapoints)
          await Datapoints.insertMany(response.data.data.datapoints);
        else
          await minioWriter.insertInDBs(response.data, {
            name: id + "-" + path.basename(new URL(urlValue).pathname),
            lastModified: new Date(),
            versionId: "null",
            isDeleteMarker: false,
            bucketName: "orion-notify",
            size: response.data.length,
            isLatest: true,
            etag: "",
            insertedBy: "orion-notify",
          });
      } else {
        let response;
        let retry = 2;
        while (retry > 0)
          try {
            response = await axios.post(
              config.mapEndpoint,
              {
                sourceDataType: "json",
                sourceDataURL: urlValue,
                decodeOptions: {
                  decodeFrom: "json-stat",
                },
                config: {
                  NGSI_entity: false,
                  ignoreValidation: true,
                  writers: [],
                  disableAjv: true,
                  mappingReport: true,
                },
                dataModel: {
                  $schema: "http://json-schema.org/schema#",
                  $id: "dataModels/DataModelTemp.json",
                  title: "DataModelTemp",
                  description: "Bike Hire Docking Station",
                  type: "object",
                  properties: {
                    region: {
                      type: "string",
                    },
                    source: {
                      type: "string",
                    },
                    timestamp: {
                      type: "string",
                    },
                    survey: {
                      type: "string",
                    },
                    dimensions: {
                      type: "object",
                    },
                    value: {
                      type: "integer",
                    },
                  },
                },
              } /*
                        {
                            //mapID,
                            sourceDataURL: urlValue
                        }*/,
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
              let outputId = response.data.id
              let lastId
              let purged = false
              for (let chunkIndex = 0; (response.data[0] || response.data.id); chunkIndex++) {
                //while (response.data[0] || response.data.id) {
                logger.info(response.data.status)
                response = await axios.get((config.sessionEdnpoint || "http://localhost:5500/api/session?") + "id=" + outputId + "&lastId=" + lastId + "&index=" + chunkIndex, {
                  headers: {
                    Authorization: `Bearer ${bearerToken}`
                  }
                })
                if (!purged)
                  await Datapoints.deleteMany({survey: response.data[0].survey});
                await Datapoints.insertMany(response.data.map(
                  d => {
                    return {
                      ...d,
                      fromUrl : urlValue
                    }
                  }
                )); //.map(d => {return {...d, dimensions : {...(d.dimensions), year : d.dimensions.time}}})) //TODO check if datapoints or other data and generalize insertion
                lastId = response.data[response.data.length - 1]?._id
                purged = true;
              }
            } catch (error) {
              logger.error("Error inserting datapoints:", error);
            }
          } catch (error) {
            logger.error(
              "Error fetching mapped data from API Connector:",
              error.response?.data || error.message
            );
            try {
              bearerToken = await updateJWT(true);
              retry--;
            } catch (e) {
              logger.error("Error updating JWT:", e);
              retry--;
            }
          }
        //logger.info(response.data.lenght)

        /*for (let i in response.data)
                    await minioWriter.insertInDBs(response.data[i], {
                        name: response.data[i].id || mapID + '-' + path.basename((new URL(urlValue)).pathname) + i,
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
      logger.info(`downloaded ${urlValue}`);
    }
    return "OK";
  },

  sync: minioWriter.sync,
};
