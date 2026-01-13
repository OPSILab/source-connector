curl --location 'http://localhost:1027/v2/subscriptions' \
  --header 'Fiware-Service: service' \
  --header 'Fiware-ServicePath: /service' \
  | jq '.[] | {id: .id, description: .description, expires: .expires}'
