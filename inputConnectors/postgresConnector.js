process.postgreInit = "busy"
const { Client } = require('pg');
const config = require('../config')
const { postgreConfig } = config
const client = new Client(postgreConfig);
const logger = require('percocologger')
client.connect();
client.query(
    `SELECT 1
             FROM information_schema.tables
             WHERE table_schema = 'public'
               AND table_name = $1`,
    ["sources"], (err, res) => {
        if (err) {
            logger.err(err)
            process.postgreInit = "done"
            return
        }
        if (!res.rows[0])
            client.query("CREATE TABLE " + "sources" + " (id SERIAL PRIMARY KEY, name TEXT, data JSONB, record JSONB)", (err, res) => {
                if (err)
                    logger.error('Error creating table:', err);
                else
                    logger.info('Table created successfully');
                process.postgreInit = "done"
            })
        else
            logger.info("Sources already exists")
        process.postgreInit = "done"
    })

module.exports = client;