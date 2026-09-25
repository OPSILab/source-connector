process.postgreInit = "busy"
const { Client } = require('pg');
const config = require('../config')
const { postgreConfig } = config
const logger = require('percocologger')
const client = new Client(postgreConfig)

function checkUserExists() {
    client.query(
        `SELECT 1 FROM pg_roles WHERE rolname = $1`,
        ['readerUser'],
        (err, result) => {
            if (err) {
                logger.error('Error checking user existence:', err)
                process.postgreInit = "done"
                return
            }

            if (result.rows.length > 0) {
                logger.info('User already exists')
                createSourcesTable()
            } else {
                setUser()
            }
        }
    )
}

function setUser() {
    const queries = [
        `CREATE USER readerUser WITH PASSWORD '${postgreConfig.password}'`,
        `GRANT CONNECT ON DATABASE ${postgreConfig.database} TO readerUser`,
        `GRANT USAGE ON SCHEMA public TO readerUser`,
        `GRANT SELECT ON ALL TABLES IN SCHEMA public TO readerUser`,
        `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readerUser`
    ]

    let index = 0

    function next() {
        if (index === queries.length) {
            logger.info('All user queries executed')
            createSourcesTable()
            return
        }

        client.query(queries[index++], (err) => {
            if (err) {
                logger.error('Error executing query:', err)
                process.postgreInit = "done"
                return
            }

            next()
        })
    }

    next()
}

function createSourcesTable() {
    client.query(
        `SELECT 1
         FROM information_schema.tables
         WHERE table_schema = 'public'
         AND table_name = $1`,
        ['sources'],
        (err, res) => {
            if (err) {
                logger.error('Error checking table existence:', err)
                process.postgreInit = "done"
                return
            }

            if (!res.rows[0]) {
                client.query(
                    `CREATE TABLE sources (
                        id SERIAL PRIMARY KEY,
                        name TEXT,
                        data JSONB,
                        record JSONB
                    )`,
                    (err) => {
                        if (err)
                            logger.error('Error creating table:', err)
                        else
                            logger.info('Table created successfully')

                        process.postgreInit = "done"
                    }
                )
            } else {
                logger.info('Sources already exists')
                process.postgreInit = "done"
            }
        }
    )
}

client.connect((err) => {
    if (err) {
        logger.error('PostgreSQL connection error:', err)
        process.postgreInit = "done"
        return
    }

    checkUserExists()
})

module.exports = client