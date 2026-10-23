// inputConnectors/apiConnector.js in a test: no API configured while it loads (it polls at once), its polling timer
// unref'd (it must not keep the test process alive). Returns pollAPI, to run a poll when the test wants.
const { load, config } = require("./env")

async function loadApiConnector() {
    const locks = load("utils/jobLocks.js")
    config.apiConnectorConfig.apiUrls = []
    const realSetInterval = global.setInterval
    global.setInterval = (...args) => realSetInterval(...args).unref()
    let pollAPI
    try {
        pollAPI = load("inputConnectors/apiConnector.js").pollAPI
    }
    finally {
        global.setInterval = realSetInterval
    }
    while (locks.polling) // the poll started when the module is loaded
        await new Promise(resolve => setTimeout(resolve, 10))
    return pollAPI
}

// A paginated API answering `pages` (limit 2): axios.get replaced, the requests recorded; failAt(offset) makes the
// request of that offset fail. restore() puts the real axios.get back.
function fakePagedApi(pages) {
    const axios = require("axios")
    const realGet = axios.get
    const api = { requests: [], failAtOffset: undefined, pages }
    axios.get = async (url, options) => {
        // offset and limit as query params (axios params) or, as before, in the url
        const offset = Number(options?.params?.offset ?? new URL(url).searchParams.get("offset"))
        api.requests.push({ offset, params: structuredClone(options?.params) }) // the connector reuses the same object for every page
        if (offset === api.failAtOffset)
            throw new Error("connection reset")
        return { data: structuredClone(api.pages[offset / 2] || []) }
    }
    api.restore = () => { axios.get = realGet }
    return api
}

module.exports = { loadApiConnector, fakePagedApi }
