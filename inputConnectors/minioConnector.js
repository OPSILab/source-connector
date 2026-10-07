const Minio = require('minio')
const common = require('../utils/common.js')
const { sleep, getEntries, setType } = common
const config = require('../config.js')
const { minioConfig, delays, queryAllowedExtensions } = config
const Key = require('../api/models/Key')
const Values = require('../api/models/Value')
const Entries = require('../api/models/Entries')
// MinIO files have their own collection (collections.minio.mongo, "minio"): everything in it comes from MinIO, so the
// sync empties it and the Status collection is no longer needed to tell MinIO documents from the others.
const { collectionSettings, collectionModel } = require('../utils/collections')
const Source = () => collectionModel("minio")
const minioSettings = () => collectionSettings("minio")
const minioClient = new Minio.Client(minioConfig)
const logger = require('percocologger')
const log = logger.info
process.queryEngine = { updatedOwners: {} }
const client = require("./postgresConnector");
const axios = require('axios')
const { writeAccumulator, removeOrigin } = require('../utils/entriesStore')
let syncing
let touchedDuringSync = new Set() // origins (re)indexed while a sync is running, never swept by that sync

// Origin used in keys/values/entries refs: unique across buckets, prefix used to tell minio origins from API urls
function minioOrigin(record) {
  return "minio://" + (record?.s3?.bucket?.name || record?.bucketName) + "/" + (record?.s3?.object?.key || record?.name)
}

function bucketOfOrigin(origin) {
  return origin.slice("minio://".length).split("/")[0]
}

let forbiddenTables = new Set(['users', 'credentials'])

// The documents of one file (same object name in the same bucket)
function fileFilter(record) {
  const bucket = record?.s3?.bucket?.name || record?.bucketName
  const name = record?.s3?.object?.key || record?.name
  return { name, $or: [{ "record.bucketName": bucket }, { "record.s3.bucket.name": bucket }] }
}

async function sync() {

  try {
    if (!syncing) {
      syncing = true
      if (minioSettings().toMongo)
        await Source().deleteMany({}) // rebuilt below from the files
      // legacy keys/values/entries written before refs existed (orphans): rebuilt below with refs
      await Key.collection.deleteMany({ refs: { $exists: false } })
      await Values.collection.deleteMany({ refs: { $exists: false } })
      await Entries.collection.deleteMany({ refs: { $exists: false } })
      touchedDuringSync.clear()
      const seenOrigins = new Set()
      const scannedBuckets = new Set()
      let objects = []
      let buckets = await listBuckets()
      let bucketIndex = 1
      for (let bucket of buckets) {
        let bucketObjects = await listObjects(bucket.name)
        scannedBuckets.add(bucket.name)
        let index = 1
        for (let obj of bucketObjects) {
          try {
            logger.debug("Bucket ", bucketIndex, " of ", buckets.length)
            logger.debug("Scanning object ", index++, " of ", bucketObjects.length, ",", obj.name)
            let extension = obj.name.split(".").pop()
            let isAllowed = (queryAllowedExtensions == "all" || queryAllowedExtensions.includes(extension))
            if (obj.size && obj.isLatest && isAllowed) {
              seenOrigins.add(minioOrigin({ name: obj.name, bucketName: bucket.name }))
              let objectGot = await getObject(bucket.name, obj.name, obj.name.split(".").pop())
              objects.push({ raw: objectGot, info: { ...obj, bucketName: bucket.name } })
            }
            else logger.info("Size is ", obj.size, ", ", (obj.isLatest ? "is latest" : "is not latest"), " and extension ", (isAllowed ? "is allowed" : "is not allowed"))
          }
          catch (error) {
            logger.error(error)
          }
        }
        logger.debug("Bucket ", bucketIndex++, " of ", buckets.length, " scanning done")
      }

      for (let obj of objects)
        try {
          await insertInDBs(obj.raw, obj.info, true) // each file: removeOrigin + write of its keys/values/entries
        }
        catch (error) {
          logger.error(error)
        }

      // sweep: files no longer in a scanned bucket (e.g. deleted while the service was down)
      const indexedOrigins = await Key.collection.distinct("refs.origin", { "refs.origin": /^minio:\/\// })
      for (const origin of indexedOrigins)
        if (origin.startsWith("minio://") && scannedBuckets.has(bucketOfOrigin(origin)) && !seenOrigins.has(origin) && !touchedDuringSync.has(origin))
          try {
            await removeOrigin(origin)
          }
          catch (error) {
            logger.error(error)
          }

      syncing = false
      logger.info("Syncing finished")
      console.info("Syncing finished")
      return "Sync finished"
    }
    else {
      logger.info("Syncing not finished")
      return "Syncing"
    }
  }
  catch (error) {
    syncing = false // release the lock, otherwise every next sync returns "Syncing" forever
    logger.error(error)
  }
}

async function listBuckets() {
  return await minioClient.listBuckets()
}

function getNotifications(bucketName) {

  const poller = minioClient.listenBucketNotification(bucketName, '', '', ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"])
  poller.on('notification', async (record) => {
    log('New object: %s/%s (size: %d)', record.s3.bucket.name, record.s3.object.key, record.s3.object.size || 0)
    let extension = record.s3.object.key.split(".").pop()
    let isAllowed = (queryAllowedExtensions == "all" || queryAllowedExtensions.includes(extension))
    let newObject
    try {
      if (record.eventName != 's3:ObjectRemoved:Delete' && record.s3.object.size && isAllowed) {
        log("Getting object")
        newObject = await getObject(record.s3.bucket.name, record.s3.object.key, record.s3.object.key.split(".").pop())
        log("Got")
      }
    }
    catch (error) {
      log("Error during getting object")
      logger.error(error)
      return
    }
    if (newObject)
      log("New object\n", common.minify(newObject), "\ntype : ", typeof newObject)
    if (isAllowed)
      if (record.eventName != 's3:ObjectRemoved:Delete')
        if (record.s3.object.size)
          await insertInDBs(newObject, record, false)
        else
          log("Size is ", record.s3.object.size || 0, " and extension ", (isAllowed ? "is allowed" : "is not allowed"))
      else
        await deleteInDBs(record)
    else
      log("Size is ", record.s3.object.size || 0, " and extension ", (isAllowed ? "is allowed" : "is not allowed"))

  })
  poller.on('error', (error) => {
    log("Error on poller")
    log(error)
  })
}

async function listObjects(bucketName) {

  let resultMessage
  let errorMessage

  let data = []
  let stream = minioClient.listObjects(bucketName, '', true, { IncludeVersion: true })
  stream.on('data', function (obj) {
    data.push(obj)
  })
  stream.on('end', function (obj) {
    if (!obj)
      log("ListObjects ended returning an empty object")
    else
      log("Found object ")
    if (data[0])
      resultMessage = data
    else if (!resultMessage)
      resultMessage = []
  })
  stream.on('error', function (err) {
    log(err)
    errorMessage = err
  })

  let logCounterFlag
  while (!errorMessage && !resultMessage) {
    await sleep(delays)
    if (!logCounterFlag) {
      logCounterFlag = true
      sleep(delays + 2000).then(resolve => {
        if (!errorMessage && !resultMessage)
          log("waiting for list")
        logCounterFlag = false
      })
    }
  }
  if (errorMessage)
    throw errorMessage
  if (resultMessage)
    return resultMessage
}

async function getObject(bucketName, objectName, format) {

  logger.trace("Now getting object " + objectName + " in bucket " + bucketName)

  let resultMessage
  let errorMessage

  minioClient.getObject(bucketName, objectName, function (err, dataStream) {
    if (err) {
      errorMessage = err
      log(err)
      return err
    }

    let objectData = '';
    dataStream.on('data', function (chunk) {
      objectData += chunk;
    });

    dataStream.on('end', function () {
      try {
        resultMessage = (format == 'json' && typeof objectData == "string") ? JSON.parse(objectData) : objectData

      }
      catch (error) {
        try {
          if (config.parseCompatibilityMode === 1)
            resultMessage = (format == 'json' && typeof objectData == "string") ? JSON.parse(objectData.substring(1)) : objectData
          else
            resultMessage = (format == 'json' && typeof objectData == "string") ? JSON.parse(objectData.substring(objectData.indexOf("{"))) : objectData
        }
        catch (error) {
          resultMessage = format == 'json' ? [{ data: objectData }] : objectData
        }
      }
      if (!resultMessage)
        resultMessage = "Empty file"
    });

    dataStream.on('error', function (err) {
      log('Error reading object:')
      errorMessage = err
      log(err)
    });

  });

  let logCounterFlag
  while (!errorMessage && !resultMessage) {
    await sleep(delays)
    if (!logCounterFlag) {
      logCounterFlag = true
      sleep(delays + 2000).then(resolve => {
        if (!errorMessage && !resultMessage)
          log("waiting for object " + objectName + " in bucket " + bucketName)
        logCounterFlag = false
      })
    }
  }
  if (errorMessage)
    throw errorMessage
  if (resultMessage)
    return resultMessage
}

// MinIO files go to MongoDB and / or PostgreSQL: collections.minio.toMongo / toPostgres
function checkQueryOptions() {
  const settings = minioSettings()
  return settings.toMongo || settings.toPostgres
}

async function insertInDBs(newObject, record, align) {
  if (!checkQueryOptions())
    return
  log("Insert in DBs ", record?.s3?.object?.key || record.name)
  let csv = false
  let jsonParsed, jsonStringified, postgreFinished, logCounterFlag
  if (typeof newObject != "object")
    try {
      jsonParsed = JSON.parse(newObject)
    }
    catch (error) {

      let extension = (record?.s3?.object?.key || record.name).split(".").pop()
      if (extension == "csv")
        jsonStringified = common.convertCSVtoJSON(newObject)
      csv = true
    }
  else {
    jsonParsed = newObject
  }

  let queryName = record?.s3?.object?.key || record.name
  let data = (jsonStringified || common.cleaned(newObject))
  if (typeof data != "string")
    data = JSON.stringify(data)
  let owner
  try {
    if (config.updateOwner == "later")
      owner = "unknown"
    else {
      if (config.minioConfig.ownerInfoEndpoint)
        owner = (await axios.get(config.minioConfig.ownerInfoEndpoint + "/createdBy?filePath=" + queryName + "&etag=" + record.etag)).data
    }
  }
  catch (error) {
    logger.error("Error getting owner")
    logger.error(error)
  }
  log("Owner ", owner)
  record = { ...record, insertedBy: owner }

  const settings = minioSettings()
  if (settings.toPostgres) {
    let table = common.urlEncode(record?.s3?.bucket?.name || record.bucketName)
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(table))
      throw new Error('Invalid table name');
    else if (forbiddenTables.has(table))
      throw new Error('Forbidden table');
    else if (table == "default")
      table = "default_table"
    else if (table == "status")
      table = "status_table"
    else if (table == "sources")
      table = "sources_table"

    //let queryTable = createTable(table)
    client.query("SELECT * FROM " + table + " WHERE name = $1", [queryName], async (err, res) => {
      if (err) {
        log("ERROR searching object in DB");
        log(err);

        client.query("CREATE TABLE " + table + " (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, data JSONB, record JSONB)", (err, res) => {

          if (err) {
            log("ERROR creating table");
            log(err);
            log("Query used :")
            log("CREATE TABLE " + table + " (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, data JSONB, record JSONB)")
            postgreFinished = true
            return;
          }

          client.query(`INSERT INTO ${table} (name, data, record) VALUES ($1, $2, $3)`, [record?.s3?.object?.key || record.name, data, JSON.stringify(record)], (err, res) => {

            if (err) {
              log("ERROR inserting object in DB");
              log(err);
              postgreFinished = true
              return;
            }
            log("Object inserted in postgres\n");
            postgreFinished = true
            return
          });

        });
        while (!postgreFinished) { //TODO create a function for this
          await sleep(delays)
          if (!logCounterFlag) {
            logCounterFlag = true
            sleep(delays + 2000).then(resolve => {
              if (!postgreFinished)
                log("waiting for inserting object in postgre")
              logCounterFlag = false
            })
          }
        }
        if (postgreFinished)
          return postgreFinished
      }
      if (res.rows[0]) {
        log("Objects found ", res.rows.length, " ", JSON.stringify(res.rows[0]).substring(0, 100), "...")//, common.minify(res.rows));
        client.query(`UPDATE ${table} SET data = $1, record = $2  WHERE name = $3`, [data, JSON.stringify(record), record?.s3?.object?.key || record.name], (err, res) => {
          if (err) {
            log("ERROR updating object in DB");
            log(err);
            postgreFinished = true
            return;
          }
          postgreFinished = true
          log("Object updated in postgres\n");
          return
        });
      }
      else
        client.query(`INSERT INTO ${table} (name, data, record) VALUES ($1, $2, $3)`, [record?.s3?.object?.key || record.name, data, JSON.stringify(record)], (err, res) => {
          if (err) {
            log("ERROR inserting object in DB");
            log(err);
            postgreFinished = true
            return;
          }
          log("Object inserted in postgres\n");
          postgreFinished = true
          return
        });

    });
  }

  if (settings.toMongo) {

    if ((!jsonParsed) || (jsonParsed && typeof jsonParsed != "object"))
      try {
        jsonParsed = JSON.parse(jsonStringified || newObject)
      }
      catch (error) {
        log(error)
      }

    try {// TODO better doing an update...
      log("Delete ", (record?.s3?.object?.key || record.name))
      await Source().deleteMany(fileFilter(record)) // the previous version of the file
    }
    catch (error) {
      log(error)
    }
    let name = record?.s3?.object?.key || record.name
    name = name.split(".")
    let extension = name.pop()
    log("Extension ", extension)
    log("Is array : ", Array.isArray(jsonParsed))
    log("Type ", typeof jsonParsed)

    if (!jsonParsed)
      log("Empty object of extension ", extension)

    let insertingSource = [
      extension == "csv" ?
        { csv: jsonParsed, record, name: record?.s3?.object?.key || record.name } :
        Array.isArray(jsonParsed) ?
          { json: jsonParsed, record, name: record?.s3?.object?.key || record.name } :
          typeof jsonParsed == "object" ?
            { ...jsonParsed, record, name: record?.s3?.object?.key || record.name } :
            { raw: jsonParsed !== undefined ? jsonParsed : newObject, record, name: record?.s3?.object?.key || record.name }
    ]
    try {
      await Source().insertMany(insertingSource)
    }
    catch (error) {
      if (!error?.errorResponse?.message?.includes("Document can't have"))
        log(error)
      try {
        await Source().insertMany(JSON.parse(JSON.stringify(insertingSource).replace(/\$/g, '')))
      }
      catch (error) {
        log("There are problems inserting object in mongo DB")
        log(error)
      }
    }
    logger.trace("before get type")
    logger.trace(JSON.stringify(jsonParsed).substring(0, 30))
    let type = await setType(extension, jsonParsed) // csv, jsonArray, json, raw
    logger.trace("type")
    logger.trace(type)
    const origin = minioOrigin(record)
    if (syncing)
      touchedDuringSync.add(origin)
    try {
      const acc = {}
      if (type != "raw")
        await getEntries(insertingSource, type, record?.s3?.object?.key || record.name, acc)
      await removeOrigin(origin)          // drops what the previous version of the file referenced
      await writeAccumulator(acc, origin, {}, "minio")
    }
    catch (error) {
      logger.error(error)
    }
  }
  while (!postgreFinished && settings.toPostgres) {
    await sleep(delays)
    if (!logCounterFlag) {
      logCounterFlag = true
      sleep(delays + 2000).then(resolve => {
        if (!postgreFinished)
          log("object inserted in mongo db but still waiting for inserting object in postgre")
        logCounterFlag = false
      })
    }
  }
  if (postgreFinished || !settings.toPostgres)
    return postgreFinished || true
}

async function deleteInDBs(record) {
  try {
    await removeOrigin(minioOrigin(record))
  }
  catch (error) {
    logger.error(error)
  }
  const settings = minioSettings()
  let postgreFinished = !settings.toPostgres, logCounterFlag
  let table = common.urlEncode(record?.s3?.bucket?.name || record.bucketName)
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(table))
    throw new Error('Invalid table name');
  else if (forbiddenTables.has(table))
    throw new Error('Forbidden table');
  else if (table == "default")
    table = "default_table"
  else if (table == "status")
    table = "status_table"
  else if (table == "sources")
    table = "sources_table"
  if (settings.toPostgres)
  client.query(`DELETE FROM ${table} WHERE name = $1`, [record?.s3?.object?.key || record.name], (err, res) => {
    if (err) {
      log("ERROR deleting object in DB");
      log(err);
      postgreFinished = true
      return;
    }
    log("Object deleted \n");
    postgreFinished = true
    return
  });

  while (!postgreFinished) {
    await sleep(delays)
    if (!logCounterFlag) {
      logCounterFlag = true
      sleep(delays + 2000).then(resolve => {
        if (!postgreFinished)
          log("Waiting for deleting object in postgre")
        logCounterFlag = false
      })
    }
  }

  try {
    log("Delete ", record?.s3?.object?.key || record.name)
    if (settings.toMongo)
      await Source().deleteMany(fileFilter(record))
  }
  catch (error) {
    log(error)
  }
}

function createTable(table, obj) {
  let query = "CREATE TABLE  " + table + " (id SERIAL PRIMARY KEY, name TEXT NOT NULL" //, type
  if (typeof obj == "string") {
    log("Now parsing")
    obj = JSON.parse(obj)
  }
  if (!Array.isArray(obj))
    for (let key in obj)
      if (Array.isArray(obj[key]))
        query = query + getTypeRecursive(obj[key])
      else
        switch (typeof obj[key]) {
          case "number": query = query + ", " + key + " INTEGER"; break;
          case "string": query = query + ", " + key + " TEXT"; break;
          case "object": query = query + ", " + key + " JSONB"; break;
          case "boolean": query = query + ", " + key + " BOOLEAN"; break;
        }
  query = query + ", record JSONB)"
  return query
}

function getTypeRecursive(obj) {
  if (!Array.isArray(obj))
    for (let key in obj)
      if (Array.isArray())
        type = "array"
      else
        switch (type = obj[key]) {
          case "number": query = query + "INTEGER,"; break;
          case "string": query = query + "TEXT,"; break;
          case "array": query = query + getTypeRecursive(obj[key]); break; // e.g. INTEGER[]
          case "object": query = query + "JSONB,"; break;
          case "boolean": query = query + "BOOLEAN"; break;
        }
}

// If MinIO isn't reachable at startup, the rejected promise used to be unhandled, which terminates the
// process (Node >= 15). Now it's logged and retried, doubling the wait up to 10 minutes.
function subscribeAllBuckets(retryDelay = 30000) {
  listBuckets().then((buckets) => {
    let a = 1
    for (let bucket of buckets) {
      getNotifications(bucket.name.toString())
      logger.debug("Subscribed bucket " + (a++) + " of " + buckets.length, "(", bucket.name, ")")
    }
  }).catch((error) => {
    logger.error(`MinIO not reachable, bucket notifications not subscribed yet: retrying in ${retryDelay / 1000}s`, error?.message || error)
    setTimeout(() => subscribeAllBuckets(Math.min(retryDelay * 2, 600000)), retryDelay)
  })
}

if (minioConfig.subscribe.all)
  subscribeAllBuckets()
else
  for (let bucket of minioConfig.subscribe.buckets)
    getNotifications(bucket)

if (!config.doNotSyncAtStart)
  sync()
if (config.syncInterval)
  setInterval(sync, config.syncInterval);

module.exports = {

  sync,

  listObjects,

  deleteInDBs,

  getTypeRecursive,

  createTable,

  getKeys(str) {
    str.split("id SERIAL PRIMARY KEY, name TEXT NOT NULL")[1].split(", record JSONB)")[0].split(",")
  },

  getValues() {

  },

  insertInDBs,

  getNotifications,

  listBuckets,

  getObject
}