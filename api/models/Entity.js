const mongoose = require("mongoose");

const entity = new mongoose.Schema({}, { strict: false, versionKey: false })

module.exports = mongoose.model("entities", entity);