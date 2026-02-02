module.exports = {
  minioConfig: {
    endPoint: 'play.min.io',
    port: 9000,
    useSSL: true,
    accessKey: 'Q3AM3UQ867SPQQA43P2F',
    secretKey: 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG',
    location: "us-east-1",
    defaultFileInput: "../../input/inputFile.json",
    defaultOutputFolderName: "private generic data",
    defaultInputFolderName: "data model mapper",
    defaultBucket: "default",
    subscribe: {
      all: true,
      buckets: []
    },
    ownerInfoEndpoint: "https://platform.beopendep.it/api/owner"
  },
  postgreConfig: {
    user: '',
    host: 'localhost',
    database: '',
    password: '',
    port: 5432
  },
  mapEndpoint: "http://localhost:5500/api/map/transform",
  mapID: "",
  orion: {
    subscribe: true,
    deleteAllDuplicateSubscriptions: true,
    attrWithUrl: "datasetUrl",
    orionBaseUrl: "http://localhost:1027",
    notificationUrl: "http://localhost:3000/api/orion/subscribe",
    fiwareService: "",
    fiwareServicePath: ""
  },
  logLevel: "info",
  syncInterval: 86400000,
  doNotSyncAtStart: false,
  delays: 1,
  queryAllowedExtensions: ["csv", "json", "geojson"],
  parseCompatibilityMode: 0,
  port: 3000,
  updateOwner: "later",
  writeLogsOnFile: true,
  mongo: "mongodb://localhost:22000/Minio-Mongo", // mongo url
  authConfig: {
    idmHost: "https://platform.beopendep.it/auth",
    clientId: "",
    username: "",
    password: "",
    userInfoEndpoint: "https://platform.beopendep.it/api/user",
    disableAuth: false,
    authProfile: "oidc",
    authRealm: "",
    introspect: false,
    publicKey: "",
    secret: "" // don't push it
  },
  sourceConnectors: {
    minioConnector: true,
    apiConnector: true
  },
  queryOptions: {
    simpleSearch: true,
    advancedSearch: true,
    SQLQuery: true,
    graphQLQuery: true
  }
}