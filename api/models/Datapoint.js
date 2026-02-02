const mongoose = require("mongoose");
const crypto = require("crypto");

// Funzione di utilità per pulire il nome della survey
const cleanSurveyName = (val) => {
  if (!val || typeof val !== "string") return val;
  return val.toUpperCase().replace(/\./g, ""); // Toglie i punti e mette in MAIUSCOLO
};

const datapointSchema = new mongoose.Schema(
  {
    source: { type: String, index: true, uppercase: true }, // uppercase: true lo fa in automatico
    survey: { type: String, index: true, uppercase: true },
    surveyName: String,
    region: { type: String, index: true, uppercase: true },
    fromUrl: String,
    timestamp: { type: String, index: true },
    dimensions: { type: Object },
    value: Number,
    d_hash: { type: String, unique: true }, // Identificatore unico per evitare duplicati
  },
  {
    strict: false,
    timestamps: true,
  }
);

// Indici per le performance
datapointSchema.index({ "dimensions.$**": 1 });
datapointSchema.index({ region: 1, timestamp: -1 });

// Middleware: Prima di ogni salvataggio, pulisce la survey
datapointSchema.pre("save", function (next) {
  if (this.survey) this.survey = cleanSurveyName(this.survey);
  next();
});

// Funzione per generare l'hash unico (usata nell'inserimento massivo)
const generateHash = (doc) => {
  const dims = doc.dimensions || {};
  const sortedKeys = Object.keys(dims)
    .sort()
    .reduce((acc, key) => {
      acc[key] = dims[key];
      return acc;
    }, {});

  // Usiamo la survey pulita per l'hash
  const s = cleanSurveyName(doc.survey);
  const stringToHash =
    (doc.fromUrl || "") +
    (doc.timestamp || "") +
    s +
    JSON.stringify(sortedKeys);
  return crypto.createHash("md5").update(stringToHash).digest("hex");
};

// Metodo per inserire milioni di record senza duplicati
datapointSchema.statics.upsertMany = async function (datapoints) {
  if (!datapoints || datapoints.length === 0) return;

  const operations = datapoints.map((doc) => {
    const hash = generateHash(doc);
    // Puliamo la survey anche qui per l'operazione di bulk
    const cleanedDoc = {
      ...doc,
      survey: cleanSurveyName(doc.survey),
      d_hash: hash,
    };
    return {
      updateOne: {
        filter: { d_hash: hash },
        update: { $set: cleanedDoc },
        upsert: true,
      },
    };
  });

  return this.bulkWrite(operations, { ordered: false });
};

module.exports = mongoose.model("Datapoint", datapointSchema);
