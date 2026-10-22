const mongoose = require("mongoose");

// Records of the paginated API polls still running (utils/pollStatus.js): { _id: <record _id>, url, pending, datePolled }.
// Collection "status" (mongoose leaves "status" as it is: named here only to make it explicit).
const status = new mongoose.Schema({}, { strict: false, versionKey: false });

module.exports = mongoose.model("status", status, "status");
