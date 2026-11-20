#!/bin/sh
# Registers every connector config mounted under /connectors — the Debezium
# Postgres source and the MinIO S3 sink both go through here.
#
# Connect accepts TCP on 8083 well before its REST routes are live, and curl's
# own --retry exits happy on any HTTP response, including the 404 that
# Connect's underlying Jetty server returns while it is still starting. So poll
# GET /connectors until it genuinely answers 200, then POST each config once.
#
# Adding another connector means mounting one more JSON file into /connectors;
# this script needs no edits.
set -u

CONNECT_URL="http://connect:8083"

wait_for_connect() {
  i=0
  while [ "$i" -lt 30 ]; do
    status=$(curl -s -o /dev/null -w '%{http_code}' "$CONNECT_URL/connectors")
    if [ "$status" = "200" ]; then
      echo "connect REST API is ready"
      return 0
    fi
    echo "waiting for connect REST API (attempt $((i + 1)), HTTP $status)"
    i=$((i + 1))
    sleep 3
  done
  echo "giving up on connect after 30 attempts, last status: $status"
  return 1
}

register() {
  config_file="$1"
  name=$(basename "$config_file")
  status=$(curl -s -o /tmp/resp.json -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    --data @"$config_file" "$CONNECT_URL/connectors")
  # 409 means a connector with this name already exists, which is the normal
  # result of a rerun (the config lives in Kafka, not in the container) and
  # not a failure.
  if [ "$status" = "201" ] || [ "$status" = "409" ]; then
    echo "$name: HTTP $status"
    return 0
  fi
  echo "$name: HTTP $status - registration FAILED"
  cat /tmp/resp.json 2>/dev/null
  echo
  return 1
}

wait_for_connect || exit 1

# Fail loudly rather than reporting success on an empty mount — a silent
# no-op here would leave the whole pipeline with no connectors at all. An
# unmatched glob stays literal in sh, so test the first expansion directly.
set -- /connectors/*.json
if [ ! -e "$1" ]; then
  echo "no connector configs found in /connectors"
  exit 1
fi

# Attempt every connector even if an earlier one fails, so one bad config
# doesn't mask the rest — but still exit non-zero so the failure is visible
# to `docker compose up` instead of being swallowed.
rc=0
for config_file in /connectors/*.json; do
  register "$config_file" || rc=1
done

exit $rc
