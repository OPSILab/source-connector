const mongoose = require("mongoose");

// The state of the incremental API polls (utils/incrementalState.js), one document per url (an API, or an element of
// a batch API): { _id: url, lastEnd, complete, missed: [{ params, at }] }. Collection "incrementalstate".
const incrementalState = new mongoose.Schema({ _id: String }, { strict: false, versionKey: false });

module.exports = mongoose.model("incrementalstate", incrementalState, "incrementalstate");
