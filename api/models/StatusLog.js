const mongoose = require("mongoose");

// HTTP requests of the Source-Connector (utils/requestLog.js): one document per request (the successes expire).
// Collection "statuslog" (named here to make it explicit).
const statusLog = new mongoose.Schema({}, { strict: false, versionKey: false });

module.exports = mongoose.model("statuslog", statusLog, "statuslog");
