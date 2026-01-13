curl --location 'http://localhost:1027/v2/subscriptions' \
  --header 'Fiware-Service: service' \
  --header 'Fiware-ServicePath: /service' \
  --header 'Content-Type: application/json' \
  --data '{
  "description": "Universal subscription (working)",
  "subject": {
    "entities": [{ "idPattern": ".*" }]
  },
  "notification": {
    "http": { "url": "http://host.docker.internal:3000/api/orion/subscribe" },
    "attrs": []
  },
  "throttling": 1
}'
