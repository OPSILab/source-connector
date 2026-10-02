const mongoose = require("mongoose");

const status = new mongoose.Schema({}, { strict: false, versionKey: false });   

module.exports = mongoose.model("status", status);