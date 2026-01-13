curl -s 'http://localhost:1027/v2/subscriptions' \
  -H 'Fiware-Service: service' \
  -H 'Fiware-ServicePath: /service' \
| jq -r '.[].id' \
| xargs -I {} curl -s -X DELETE "http://localhost:1027/v2/subscriptions/{}" \
  -H 'Fiware-Service: service' \
  -H 'Fiware-ServicePath: /service'
