const mongoose = require("mongoose");

// The resets of the incremental APIs (utils/resetIncremental.js), one document per reset, kept forever:
// { startedAt, finishedAt, by, apis: [names], result: [{ name, url, before, after }], outcome: running | done | error,
//   error }. Collection "incrementalresetlog". GET /api/incremental/reset/log lists them.
const incrementalResetLog = new mongoose.Schema({}, { strict: false, versionKey: false });

module.exports = mongoose.model("incrementalresetlog", incrementalResetLog, "incrementalresetlog");
