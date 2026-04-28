const mongoose = require("mongoose");

const dimensions
 = new mongoose.Schema({}, { strict: false, versionKey: false });   

module.exports = mongoose.model("dimensions", dimensions);