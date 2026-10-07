// Test environment for the Source-Connector unit tests (node --test).
//
// - percocologger is silenced and never writes log files (TEST_LOG_LEVEL=debug to see the logs);
// - config.js is replaced by a copy of config.template.js (config.js is gitignored and machine-specific),
//   tests change it through `config` and restore it with resetConfig(); setBaseConfig() changes what
//   resetConfig() restores (db.js uses it for the test databases);
// - stub(relPath, exports) replaces a project module (models, pg connector, ...) before it is required,
//   even if the real file does not exist.
//
// Require this file FIRST in every test file, before any project module.

const path = require("path")
const os = require("os")
const Module = require("module")

const ROOT = path.resolve(__dirname, "../..")

process.percocologger = {
    logPath: path.join(os.tmpdir(), "source-connector-test-logs") + path.sep,
    logLevel: process.env.TEST_LOG_LEVEL || "silent",
    saveLog: false,
    showFunctionName: false,
    preMessage: "",
    initialFrame: "",
    finalFrame: "",
    maxLogSize: 200,
    maxSingleLogSize: 100
}
process.env.LEVEL = process.percocologger.logLevel

const virtual = new Set()
const originalResolve = Module._resolveFilename
Module._resolveFilename = function (request, parent, ...rest) {
    if (parent?.filename && (request.startsWith("./") || request.startsWith("../"))) {
        const absolute = path.resolve(path.dirname(parent.filename), request)
        for (const candidate of [absolute, absolute + ".js"])
            if (virtual.has(candidate))
                return candidate
    }
    return originalResolve.call(this, request, parent, ...rest)
}

function stub(relPath, exports) {
    const file = path.join(ROOT, relPath)
    virtual.add(file)
    const mod = new Module(file)
    mod.filename = file
    mod.loaded = true
    mod.exports = exports
    require.cache[file] = mod
    return exports
}

const clone = value => JSON.parse(JSON.stringify(value))
const template = require(path.join(ROOT, "config.template.js"))
const config = stub("config.js", clone(template))

// In place, nested objects included: modules that kept a reference (e.g. `const authConfig = config.authConfig`)
// see the restored values too.
function restore(target, source) {
    for (const key of Object.keys(target))
        if (!(key in source))
            delete target[key]
    for (const [key, value] of Object.entries(source))
        if (value && typeof value === "object" && !Array.isArray(value) && target[key] && typeof target[key] === "object" && !Array.isArray(target[key]))
            restore(target[key], value)
        else
            target[key] = clone(value)
}

const base = clone(template)

function resetConfig() {
    restore(config, base)
    return config
}

// Top-level keys of `patch` replace those of the base config (and of the current one)
function setBaseConfig(patch) {
    Object.assign(base, clone(patch))
    return resetConfig()
}

function load(relPath) {
    return require(path.join(ROOT, relPath))
}

module.exports = { ROOT, stub, load, config, resetConfig, setBaseConfig }
