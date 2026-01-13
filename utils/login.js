
const axios = require('axios');

const KEYCLOAK_URL = 'https://platform.beopendep.it/auth';
const REALM = 'master';             
const CLIENT_ID = '';     
const CLIENT_SECRET = ''; 

async function authenticate() {
  const tokenUrl = `${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token`;

  const params = new URLSearchParams();
  params.append('grant_type', 'client_credentials');
  params.append('client_id', CLIENT_ID);
  params.append('client_secret', CLIENT_SECRET);

  try {
    const response = await axios.post(tokenUrl, params, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    });

    console.log('Access Token:', response.data.access_token);
    return "Bearer " + response.data.access_token;
  } catch (err) {
    console.error('Errore durante l\'autenticazione:', err.response?.data || err.message);
    return null;
  }
}

module.exports = {
    authenticate
}

