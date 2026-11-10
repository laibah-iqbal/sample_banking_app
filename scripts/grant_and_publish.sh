#!/bin/sh
# Runs as its own container (not a docker-entrypoint-initdb.d script) because
# it depends on the app's tables existing, which only happens once the app
# container starts and runs create_db_and_tables() — long after Postgres's
# own init scripts have finished. Retries until the tables show up, same
# pattern as register-connector.sh retrying until Connect is actually ready.
i=0
while [ "$i" -lt 30 ]; do
  exists=$(psql -h postgres -U "$DATABASE_USERNAME" -d "$DATABASE_NAME" -tAc \
    "SELECT to_regclass('public.transactions') IS NOT NULL;")
  if [ "$exists" = "t" ]; then
    psql -h postgres -U "$DATABASE_USERNAME" -d "$DATABASE_NAME" -v ON_ERROR_STOP=1 <<-EOSQL
      GRANT SELECT ON transactions, ledger_entries, user_accountactivity TO debezium;
      DROP PUBLICATION IF EXISTS debezium_publication;
      CREATE PUBLICATION debezium_publication FOR TABLE transactions, ledger_entries, user_accountactivity;
EOSQL
    echo "grants and publication created"
    exit 0
  fi
  echo "attempt $((i + 1)): tables not ready yet"
  i=$((i + 1))
  sleep 2
done
echo "giving up after 30 attempts: tables never showed up"
exit 1
