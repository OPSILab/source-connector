const mongoose = require("mongoose");

const value = new mongoose.Schema(
    {
        value: { type: mongoose.Schema.Types.Mixed, required: true },
        visibility: { type: Array, required: true },
    },
    { versionKey: false }
);

module.exports = mongoose.model("value", value);