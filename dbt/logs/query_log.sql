-- created_at: 2026-11-27T07:59:45.235434+00:00
-- finished_at: 2026-11-27T07:59:45.240684+00:00
-- elapsed: 5ms
-- outcome: success
-- dialect: duckdb
-- node_id: not available
-- query_id: not available
-- desc: dbt run query

  
  select column_name, column_type from (describe select after, source from read_parquet('s3://bronze/topics/transactions.public.transactions/event_date=*/*.parquet', hive_partitioning=true, union_by_name=true))
  
  limit 10
;

