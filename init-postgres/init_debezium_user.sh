#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER debezium WITH REPLICATION LOGIN PASSWORD '$DEBEZIUM_PASSWORD';
  GRANT SELECT ON transactions, ledger_entries, accountactivity TO debezium;
  CREATE PUBLICATION debezium_publication FOR TABLE transactions, ledger_entries;
EOSQL
