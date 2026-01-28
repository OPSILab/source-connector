const axios = require("axios");
const config = require("../config.js")
const keycloakBaseUrl = config.authConfig.idmHost + "/realms/smartera";
const { clientId, username, password } = config.authConfig;
const fs = require('fs');
const path = './token.js';
let token

async function updateJWT(update) {

    if (!update)
        try {
            token = require("." + path);
            return token;
        } catch (error) {
            console.error("Error loading token:", error);
        }
    console.log("Update token")
    const response = await axios.post(
        `${keycloakBaseUrl}/protocol/openid-connect/token`,
        new URLSearchParams({
            grant_type: "password",
            client_id: clientId,
            username,
            password,
        }),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );
    console.log(response.data.access_token);
    fs.writeFileSync(path, 'module.exports = "' + response.data.access_token + '"', 'utf8');
    return response.data.access_token;
}

module.exports = { updateJWT };
