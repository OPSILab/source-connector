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

module.exports = { CHECKED_FIELDS, plainValue, fieldChanged, changedFields, mustUpdate }
