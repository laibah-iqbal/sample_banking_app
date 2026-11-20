#!/bin/bash
set -e
# Only the role is created here. GRANT/CREATE PUBLICATION need the app's
# tables to exist first, and this script runs during Postgres's own first
# boot — before the app container has even started, let alone run
# create_db_and_tables(). See grant_and_publish.sh for the rest.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER debezium WITH REPLICATION LOGIN PASSWORD '$DEBEZIUM_PASSWORD';
EOSQL
