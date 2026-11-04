// managementSecret (config): the password that the destructive operations require besides the login - here the
// reset of the incremental APIs (POST /incremental/reset; its dry run does not need it). The Query-Engine asks the
// same for deleting cache backups: set the same value in its config. Sent in the X-Management-Secret header, compared
// in constant time; not configured (empty): those operations are disabled.
const crypto = require("crypto")
const config = require("../config")

const HEADER = "x-management-secret"

const configured = () => typeof config.managementSecret == "string" && config.managementSecret.length > 0

function same(given, expected) {
    if (typeof given != "string")
        return false
    const a = crypto.createHash("sha256").update(given).digest()
    const b = crypto.createHash("sha256").update(expected).digest()
    return crypto.timingSafeEqual(a, b)
}

// Why the request is refused (403), or undefined
function refusal(req) {
    if (!configured())
        return "Disabled: managementSecret is not set in the configuration of the Source-Connector"
    if (!same(req.headers?.[HEADER], config.managementSecret))
        return "Wrong or missing management password (X-Management-Secret)"
    return undefined
}

// Who asked: the e-mail (or username) of the token, if any
function requester(req) {
    if (config.authConfig?.disableAuth)
        return "authentication disabled"
    try {
        const token = (req.headers?.authorization || "").replace(/^Bearer\s+/i, "")
        const payload = token ? JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString()) : {}
        return payload.email || payload.preferred_username || "unknown"
    }
    catch {
        return "unknown"
    }
}

module.exports = { configured, refusal, requester, HEADER }
