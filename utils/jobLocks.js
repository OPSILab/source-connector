// In-process coordination between apiConnector polls and on-demand rebuilds (utils/rebuild.js):
// a poll tick is skipped while a rebuild runs, a rebuild is refused while a poll runs.
module.exports = {
    polling: false,
    rebuilding: false
}
