// GET /api/config: what anyone may see of the configuration (no authentication, read only): the connectors enabled
// and the APIs polled - name, url, method, paginated / batch, incremental with its start date and maxWindow - and the
// poll interval. Only the fields listed here: never headers, credentials, tokens, query params or bodies. In the urls
// the user and password are dropped and the values of the query params that look like credentials are hidden.
const config = require("../config")

const SENSITIVE = /pass|secret|token|key|auth|credential|signature|session|cookie/i

function safeUrl(url) {
    if (typeof url != "string" || !url)
        return undefined
    let parsed
    try {
        parsed = new URL(url)
    }
    catch {
        return "<not a valid url>"
    }
    parsed.username = ""
    parsed.password = ""
    for (const name of [...new Set(parsed.searchParams.keys())])
        if (SENSITIVE.test(name))
            parsed.searchParams.set(name, "masked")
    // {batch} as written in the configuration
    return parsed.toString().replace(/%7B/gi, "{").replace(/%7D/gi, "}")
}

function publicApi(api) {
    const params = api.incrementalParams || {}
    return {
        name: typeof api.name == "string" ? api.name : undefined,
        url: safeUrl(api.url),
        method: typeof api.method == "string" ? api.method.toUpperCase() : "GET",
        paginated: !!api.pagination,
        ...(api.batch ? { batch: { from: safeUrl(api.batch.from) } } : {}),
        incremental: !!api.incremental,
        ...(api.incremental ? {
            startDateValue: params.startDateValue ?? null,
            maxWindow: Number.isFinite(params.maxWindow) && params.maxWindow > 0 ? params.maxWindow : null,
            // maxWindow needs an end date param to split the requests (see apiConnector.maxWindowOf)
            ...(Number.isFinite(params.maxWindow) && params.maxWindow > 0 && !params.endDateParam ? { maxWindowIgnored: true } : {})
        } : {})
    }
}

function publicConfig(cfg = config) {
    const sources = cfg.sourceConnectors || {}
    return {
        sources: {
            minio: !!sources.minioConnector,
            orion: !!sources.orionConnector,
            api: !!sources.apiConnector
        },
        pollInterval: Number.isFinite(cfg.apiConnectorConfig?.pollInterval) ? cfg.apiConnectorConfig.pollInterval : null,
        apis: (Array.isArray(cfg.apiConnectorConfig?.apiUrls) ? cfg.apiConnectorConfig.apiUrls : []).filter(api => api && typeof api == "object").map(publicApi)
    }
}

module.exports = { publicConfig, safeUrl }
