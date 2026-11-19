// When an Orion DistributionDCAT-AP entity must be mapped again (common.checkMustUpdateDistributionDcatAp, used by the
// daily check and by the notifications). No I/O here, tested in test/distributionCheck.test.js.

const CHECKED_FIELDS = ["modifiedDate", "byteSize", "checksum"]

// The plain value of an attribute, however it is wrapped: Orion ({ type, value }), JSON-LD ({ "@type", "@value" }),
// the entities collection (a mongoose document), or fastAndPartialOrionizeEntity ({ value: { value } }).
// As a string ("" = no value): byteSize 1634109 and "1634109" are the same.
function plainValue(attribute) {
    let value = attribute
    for (let depth = 0; depth < 10 && value && typeof value == "object" && !Array.isArray(value); depth++)
        if ("@value" in value)
            value = value["@value"]
        else if ("value" in value)
            value = value.value
        else
            break
    if (value === undefined || value === null)
        return ""
    return typeof value == "object" ? JSON.stringify(value) : String(value).trim()
}

const plain = entity => (entity && typeof entity.toObject == "function" ? entity.toObject() : entity)

// The new entity has a value, different from the stored one (a value that disappears is not a change)
function fieldChanged(entity, stored, field) {
    const value = plainValue(entity?.[field])
    return value !== "" && value !== plainValue(stored?.[field])
}

// [fields that changed]; every field when nothing is stored
function changedFields(entity, stored) {
    stored = plain(stored)
    if (!stored)
        return [...CHECKED_FIELDS]
    return CHECKED_FIELDS.filter(field => fieldChanged(entity, stored, field))
}

const mustUpdate = (entity, stored) => changedFields(entity, stored).length > 0

// orion.skippingParams: { <attribute>: [values] }. A value is the exact text of the attribute (case sensitive), a
// regular expression (a RegExp in config.js), or a text "regex:<pattern>" (the pattern of a RegExp, e.g.
// "regex:/sdmx/2\\.1/dataflow/" for every Eurostat metadata url). An invalid pattern never matches (warned once).
const REGEX_PREFIX = "regex:"
const compiled = new Map()
function patternOf(text, warn) {
    if (!compiled.has(text)) {
        let regex = null
        try {
            regex = new RegExp(text.slice(REGEX_PREFIX.length))
        }
        catch (error) {
            warn?.(`orion.skippingParams: invalid pattern ${JSON.stringify(text)} ignored (${error.message})`)
        }
        compiled.set(text, regex)
    }
    return compiled.get(text)
}

function matchesSkipping(value, item, warn) {
    if (item instanceof RegExp)
        return item.test(value)
    if (typeof item == "string" && item.startsWith(REGEX_PREFIX))
        return !!patternOf(item, warn)?.test(value)
    return item == value
}

// The attribute of the entity matching orion.skippingParams (the entity is not synchronized when new), else undefined
function skippingMatch(entity, skippingParams, warn) {
    for (const [attribute, items] of Object.entries(skippingParams || {})) {
        if (!Array.isArray(items))
            continue
        const value = plainValue(entity?.[attribute])
        if (items.some(item => matchesSkipping(value, item, warn)))
            return attribute
    }
    return undefined
}

module.exports = { CHECKED_FIELDS, plainValue, fieldChanged, changedFields, mustUpdate, skippingMatch }
