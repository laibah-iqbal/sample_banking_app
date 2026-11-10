import logging
import os
import sys

from pyflink.common import Types, WatermarkStrategy
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

from . import utils
from app.database import schemas

def consume_login_events(env: TableEnvironment):
    account_activity= env.execute_sql(utils.debezium_envelope_ddl(schemas.UserAccountActivity,
                                                                  topic="transactions.public.user_accountactivity", 
                                                                  watermark_col="created_at",
                                                                  bootstrap_servers='localhost:9092',
                                                                  group_id='testGroup'))
    
    transactions= env.execute_sql(utils.debezium_envelope_ddl(schemas.Transaction,
                                                                      topic="transactions.public.transactions", 
                                                                      watermark_col="created_at",
                                                                      bootstrap_servers='localhost:9092',
                                                                      group_id='testGroup'))
    
    # COUNT(...) over a join is an updating aggregate — the count for a given
    # transaction_id can retract and re-emit as more matching login rows
    # arrive (1 -> 2 -> 3), even though the join feeding it is append-only.
    # A plain 'kafka' sink only accepts inserts, so this needs upsert-kafka,
    # which consumes that changelog by writing the latest value per key.
    sink_table = env.execute_sql("""
        CREATE TABLE enriched_stream(
            transaction_id VARCHAR,
            user_id VARCHAR,
            transaction_type VARCHAR,
            amount DECIMAL(18,2),
            login_attempts_last_30min BIGINT,
            PRIMARY KEY (transaction_id, user_id, transaction_type, amount) NOT ENFORCED
    
        ) WITH (
          'connector' = 'upsert-kafka',
          'topic' = 'transaction_login_attempts',
          'properties.bootstrap.servers' = 'localhost:9092',
          'key.format' = 'json',
          'value.format' = 'json'
        )
        """)
    
    
    env.sql_query("""WITH t AS (
            SELECT after.id AS transaction_id,
                after.initiated_by AS user_id,
                after.txn_type as transaction_type,
                CAST(after.amount AS DECIMAL(18,2)) as amount,
                created_at_ts
            FROM transactions
            WHERE op = 'c'
        )
        SELECT t.transaction_id,
            t.user_id,
            t.transaction_type,
            t.amount,
            COUNT(a.after.id) AS login_attempts_last_30min
        FROM t
        LEFT JOIN user_accountactivity AS a
        ON a.after.initiated_by = t.user_id
        AND a.op = 'c'
        AND a.after.activity = 'login'
        AND a.after.activity_outcome = 'success'
        AND a.created_at_ts BETWEEN t.created_at_ts - INTERVAL '30' MINUTE AND t.created_at_ts
        GROUP BY t.transaction_id, t.user_id, t.transaction_type, t.amount
        """) \
    .execute_insert("enriched_stream").wait()


if __name__ == "__main__":

    # 1. Initialize the Flink Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # 2. Add required Kafka connector JARs to the execution environment
    # Must be a local file:// path, not a remote https:// URL — job
    # submission uploads this jar to the MiniCluster's blob store via
    # Flink's own FileSystem abstraction, which has no plugin for https
    # (only file://, hdfs://, s3://, etc.). Pre-downloaded into flink/lib/.
    jar_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "lib", "flink-sql-connector-kafka-5.0.0-2.2.jar",
    )
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", f"file://{jar_path}"
    )
    
    t_env.get_config().set("table.exec.state.ttl", "86400000")

    # The embedded MiniCluster's internal components (JobManager RPC,
    # BlobServer, TaskManager data exchange) resolve this Mac's hostname to
    # a LAN IP by default — and that IP can change between the components
    # binding and connecting to each other, causing "Could not connect to
    # BlobServer" timeouts. Forcing everything to loopback avoids relying
    # on that resolution at all.
    for key in ("rest.bind-address", "rest.address",
                "jobmanager.bind-host", "jobmanager.rpc.address",
                "taskmanager.bind-host", "taskmanager.host"):
        t_env.get_config().get_configuration().set_string(key, "localhost")

    consume_login_events(t_env)


