-- Smoke test, not a real staging model: the only question it answers is
-- whether dbt/DuckDB can reach MinIO and parse a bronze object at all.
--
-- select * on purpose. The Debezium envelope comes back as nested structs
-- (op, before, after, source, ts_ms/ts_us/ts_ns, transaction) plus event_date
-- from the Hive partition, so this is also the quickest way to see the shape
-- the real silver models will have to unnest.
select *
from {{ source('minio_raw', 'transactions') }}

