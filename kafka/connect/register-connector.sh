#!/bin/sh
# Registers the Debezium connector, retrying until Connect's REST routes are
# actually live (not just its port open) rather than trusting curl's own
# connection-level retry, which exits happy on any HTTP response including a
# "not ready yet" 404 from Connect's underlying Jetty server.
i=0
while [ "$i" -lt 30 ]; do
  status=$(curl -s -o /tmp/resp.json -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    --data @/connector.json http://debezium:8083/connectors)
  echo "attempt $((i + 1)): HTTP $status"
  if [ "$status" = "201" ] || [ "$status" = "409" ]; then
    cat /tmp/resp.json
    exit 0
  fi
  i=$((i + 1))
  sleep 3
done
echo "giving up after 30 attempts, last status: $status"
cat /tmp/resp.json 2>/dev/null
exit 1
