const mongoose = require("mongoose");

// Failed HTTP requests of the Source-Connector (utils/requestLog.js): one document per request, kept forever.
// Collection "statuslog" (named here to make it explicit).
const statusLog = new mongoose.Schema({}, { strict: false, versionKey: false });

module.exports = mongoose.model("statuslog", statusLog, "statuslog");
