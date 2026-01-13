const common = require("./utils/common")
const config = common.checkConfig(require('./config'), require('./config.template'))
const mongoose = require("mongoose");
mongoose.connect(config.mongo, { useNewUrlParser: true, useUnifiedTopology: true }).then(() => {
    const express = require('express');
    const bodyParser = require('body-parser');
    const app = express();
    const port = config.port;
    const cors = require('cors');
    const logger = require('percocologger')
    logger.info(config.queryAllowedExtensions);
    logger.info("Connected to mongo")
    const routes = require("./api/routes/router")
    app.use(cors());
    app.use(express.urlencoded({ extended: false }));
    app.use(bodyParser.json());
    app.use(config.basePath || "/api", routes);
    app.listen(port, () => { logger.info(`Source connector server listens on http://localhost:${port}`); });
    logger.info(`Node.js version: ${process.version}`);
})
