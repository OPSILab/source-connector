const mongoose = require("mongoose");

const source = new mongoose.Schema({}, { strict: false, versionKey: false });   

module.exports = mongoose.model("source", source);