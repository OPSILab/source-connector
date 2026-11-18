// hideFields of an API (apiConnectorConfig.apiUrls): the values at those paths in its answers replaced by a pseudonym,
// before anything is stored or searched. A path is dotted and goes through arrays as MongoDB does:
// "entry.resource.participant.actor.display" is the display of the actor of every participant of every entry (a
// numeric segment is that element only: "entry.0.…"); applied to every record of an answer that is a list.
// The pseudonym is "anon:" + HMAC-SHA256 of the value with the key hideFieldsSecret (config), 32 hex characters: the
// same value gives the same pseudonym (the records of the same person stay linked), and without the key nothing leads
// back to the value (a plain hash of a name would be found by trying names). A value that is not text is hashed as its
// JSON; an array of values: each one; null / missing: left as it is; a pseudonym already: not hashed again.
// The same file is in the Source-Connector (utils/hideFields.js) and the Query-Engine (api/services/hideFields.js):
// both need the same hideFieldsSecret, so that the live Simple search shows the pseudonyms stored.
const crypto = require("crypto")

const PREFIX = "anon:"
const PSEUDONYM = /^anon:[0-9a-f]{32}$/

// The paths of an API to hide ([] when none); a path that is not a dotted text: an error
function hiddenPaths(api) {
    const paths = api?.hideFields
    if (paths === undefined || paths === null)
        return []
    const list = Array.isArray(paths) ? paths : [paths]
    for (const path of list)
        if (typeof path != "string" || !path || path.split(".").some(segment => !segment))
            throw Object.assign(new Error(`${api.name}: hideFields must be dotted paths (e.g. entry.resource.participant.actor.display), not ${JSON.stringify(path)}`), { code: "INVALID" })
    return list
}

function pseudonym(value, secret) {
    if (value === null || value === undefined)
        return value
    if (Array.isArray(value))
        return value.map(item => pseudonym(item, secret))
    if (typeof value == "string" && PSEUDONYM.test(value))
        return value
    const text = typeof value == "string" ? value : JSON.stringify(value)
    return PREFIX + crypto.createHmac("sha256", secret).update(text).digest("hex").slice(0, 32)
}

function hideAt(target, segments, secret) {
    const [key, ...rest] = segments
    if (Array.isArray(target)) {
        if (!/^\d+$/.test(key)) {
            for (const element of target)
                hideAt(element, segments, secret)
            return
        }
        const index = Number(key)
        if (index >= target.length)
            return
        if (rest.length)
            hideAt(target[index], rest, secret)
        else
            target[index] = pseudonym(target[index], secret)
        return
    }
    if (target === null || typeof target != "object" || !Object.prototype.hasOwnProperty.call(target, key))
        return
    if (rest.length)
        hideAt(target[key], rest, secret)
    else
        target[key] = pseudonym(target[key], secret)
}

// The data of an answer with the hideFields of the API hidden, in place (returned too). No secret: an error (nothing
// must be stored or shown with the values in clear).
function hideFields(data, api, secret) {
    const paths = hiddenPaths(api)
    if (!paths.length)
        return data
    if (typeof secret != "string" || !secret)
        throw Object.assign(new Error(`${api.name}: hideFields needs hideFieldsSecret in the configuration`), { code: "NO_SECRET" })
    for (const path of paths)
        hideAt(data, path.split("."), secret)
    return data
}

module.exports = { hideFields, hiddenPaths, pseudonym, PSEUDONYM }
