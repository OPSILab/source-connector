const mongoose = require("mongoose");

const key = new mongoose.Schema({}, { strict: false, versionKey: false })

module.exports = mongoose.model("entries", key);