const common = require("./utils/common")
const ent = {
    pre: {
        "id": "urn:ngsi-ld:DistributionDCAT-AP:id:7b6b9015-337a-49fe-a4da-da5c5a26f29f",
        "type": "DistributionDCAT-AP",
        "description": "T l charger l ensemble de donn es au format SDMX 2 1",
        "title": "                                       SDMX 2 1",
        "accessUrl": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10r_3gdp?format=sdmx_2.1_structured&compressed=true",
        "byteSize": "1320246",
        "checksum": "be935b792a7a934514edf2247f6dd7811485a9d93d86fc96e5f68d89cdea42c5",
        "downloadURL": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10r_3gdp?format=sdmx_2.1_structured&compressed=true",
        "language": [],
        "license": "",
        "mediaType": "application/xml",
        "rights": "http://publications.europa.eu/resource/authority/access-right/PUBLIC",
        "format": "XML",
        "status": ""
    },
    post: {
        id: 'urn:ngsi-ld:DistributionDCAT-AP:id:7b6b9015-337a-49fe-a4da-da5c5a26f29f',
        type: 'DistributionDCAT-AP',
        description: { value: 'T l charger l ensemble de donn es au format SDMX 2 1' },
        title: { value: '                                       SDMX 2 1' },
        accessUrl: {
            value: 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10r_3gdp?format=sdmx_2.1_structured&compressed=true'
        },
        byteSize: { value: '1320246' },
        checksum: {
            value: 'be935b792a7a934514edf2247f6dd7811485a9d93d86fc96e5f68d89cdea42c5'
        },
        downloadURL: {
            value: 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10r_3gdp?format=sdmx_2.1_structured&compressed=true'
        },
        language: { value: [] },
        license: { value: '' },
        mediaType: { value: 'application/xml' },
        rights: {
            value: 'http://publications.europa.eu/resource/authority/access-right/PUBLIC'
        },
        format: { value: 'XML' },
        status: { value: '' }
    }
}
const existingEntity = {
    pre: {
        "id": "urn:ngsi-ld:DistributionDCAT-AP:id:7b6b9015-337a-49fe-a4da-da5c5a26f29f",
        "type": "DistributionDCAT-AP",
        "description": {
            "type": "Property",
            "value": "T l charger l ensemble de donn es au format SDMX 2 1"
        },
        "title": {
            "type": "Property",
            "value": "                                       SDMX 2 1"
        },
        "accessUrl": {
            "type": "Property",
            "value": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10r_3gdp?format=sdmx_2.1_structured&compressed=true"
        },
        "byteSize": {
            "type": "Property",
            "value": "1320246"
        },
        "checksum": {
            "type": "Property",
            "value": "be935b792a7a934514edf2247f6dd7811485a9d93d86fc96e5f68d89cdea42c5"
        },
        "downloadURL": {
            "type": "Property",
            "value": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10r_3gdp?format=sdmx_2.1_structured&compressed=true"
        },
        "language": {
            "type": "Property",
            "value": []
        },
        "license": {
            "type": "Property",
            "value": ""
        },
        "mediaType": {
            "type": "Property",
            "value": "application/xml"
        },
        "rights": {
            "type": "Property",
            "value": "http://publications.europa.eu/resource/authority/access-right/PUBLIC"
        },
        "format": {
            "type": "Property",
            "value": "XML"
        },
        "status": {
            "type": "Property",
            "value": ""
        }
    }
}
existingEntity.post = existingEntity.pre

console.log("First match : ")
/*console.log(
    JSON.stringify(common.fastAndPartialOrionizeEntity(ent.pre)),
    "\n",
    JSON.stringify(ent.post),
    "\n\n"
)*/
console.assert(JSON.stringify(common.fastAndPartialOrionizeEntity(ent.pre)) === JSON.stringify(ent.post))
console.log("Second match : ")
/*console.log(
    JSON.stringify(common.fastAndPartialOrionizeEntity(existingEntity.pre)),
    "\n",
    JSON.stringify(existingEntity.post),
    "\n\n"
)*/
console.assert(JSON.stringify(common.fastAndPartialOrionizeEntity(existingEntity.pre)) === JSON.stringify(existingEntity.post))
process.exit()