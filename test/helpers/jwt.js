// RS256 key pair + token factory for the auth middleware tests (no Keycloak needed).
const { generateKeyPairSync } = require("crypto")
const jwt = require("jsonwebtoken")

const { publicKey, privateKey } = generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: { type: "spki", format: "pem" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" }
})
const other = generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: { type: "spki", format: "pem" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" }
})

// claims: azp, email, ... ; expiresIn in seconds (negative = already expired)
function makeToken(claims = {}, { expiresIn = 3600, key = privateKey } = {}) {
    const now = Math.floor(Date.now() / 1000)
    return jwt.sign({ iat: now - 10, exp: now + expiresIn, ...claims }, key, { algorithm: "RS256" })
}

module.exports = { publicKey, privateKey, otherPrivateKey: other.privateKey, makeToken }
