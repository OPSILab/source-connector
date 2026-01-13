const { Client } = require('pg');
const config = require('../config')
const { postgreConfig } = config
const client = new Client(postgreConfig);
client.connect();
module.exports = client;