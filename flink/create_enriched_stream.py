import logging
import os
import sys

from pyflink.common import Types, WatermarkStrategy
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

from . import utils
from app.database import schemas

# localhost:9092 is the host-facing listener, which is right when this runs on
# the host against the published port. Inside the compose network it has to be
# kafka:19092 — the PLAINTEXT listener — because localhost there is the job's
# own container. The compose service sets this.
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Whether to run an embedded MiniCluster in this process (the host dev loop) or
# submit to a real cluster (`flink run` against the jobmanager). The two need
# opposite configuration, so it cannot be inferred safely: the MiniCluster path
# needs pipeline.jars and loopback addresses, and setting either when
# submitting to a cluster is actively wrong.
EMBEDDED = os.environ.get("FLINK_EMBEDDED", "1").lower() in ("1", "true", "yes")

def consume_login_events(env: TableEnvironment):
    account_activity= env.execute_sql(utils.debezium_envelope_ddl(schemas.UserAccountActivity,
                                                                  topic="transactions.public.user_accountactivity",
                                                                  watermark_col="created_at",
                                                                  bootstrap_servers=BOOTSTRAP_SERVERS,
                                                                  group_id='testGroup'))

    transactions= env.execute_sql(utils.debezium_envelope_ddl(schemas.Transaction,
                                                                      topic="transactions.public.transactions",
                                                                      watermark_col="created_at",
                                                                      bootstrap_servers=BOOTSTRAP_SERVERS,
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
          'properties.bootstrap.servers' = '""" + BOOTSTRAP_SERVERS + """',
          'key.format' = 'json',
          'value.format' = 'json'
        )
        """)
    
    
    # Every envelope field is addressed through `payload` because the Connect
    # worker runs with CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=true, which wraps
    # each message as {"schema": ..., "payload": {before, after, op, ts_ms}}.
    # See the note in utils.debezium_envelope_ddl — getting this wrong does not
    # raise, it just silently yields zero rows.
    result = env.sql_query("""WITH t AS (
            SELECT payload.after.id AS transaction_id,
                payload.after.initiated_by AS user_id,
                payload.after.txn_type as transaction_type,
                CAST(payload.after.amount AS DECIMAL(18,2)) as amount,
                created_at_ts
            FROM transactions
            WHERE payload.op = 'c'
        )
        SELECT t.transaction_id,
            t.user_id,
            t.transaction_type,
            t.amount,
            COUNT(a.payload.after.id) AS login_attempts_last_30min
        FROM t
        LEFT JOIN user_accountactivity AS a
        ON a.payload.after.initiated_by = t.user_id
        AND a.payload.op = 'c'
        AND a.payload.after.activity = 'login'
        AND a.payload.after.activity_outcome = 'success'
        AND a.created_at_ts BETWEEN t.created_at_ts - INTERVAL '30' MINUTE AND t.created_at_ts
        GROUP BY t.transaction_id, t.user_id, t.transaction_type, t.amount
        """) \
    .execute_insert("enriched_stream")

    # wait() blocks on the job's final result, which is what keeps the embedded
    # MiniCluster's process (and therefore the cluster itself) alive — without
    # it the interpreter exits and takes the job with it.
    #
    # It must NOT be called for a detached submission. `flink run -d` returns a
    # JobClient that cannot retrieve a result, so wait() raises, the Python
    # process exits 1, and the CLI reports "Python process exits with code: 1"
    # *after* having successfully submitted. Compose then treats that as a
    # failed container and restarts it, submitting a duplicate job on every
    # retry — the cluster ends up running several copies of the same pipeline,
    # all writing to the same topic.
    if EMBEDDED:
        result.wait()


if __name__ == "__main__":

    # 1. Initialize the Flink Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().set("table.exec.state.ttl", "86400000")

    if EMBEDDED:
        # 2. Add required Kafka connector JARs to the execution environment
        # Must be a local file:// path, not a remote https:// URL — job
        # submission uploads this jar to the MiniCluster's blob store via
        # Flink's own FileSystem abstraction, which has no plugin for https
        # (only file://, hdfs://, s3://, etc.). Pre-downloaded into flink/lib/.
        #
        # Only for the embedded path. On a real cluster the jar already sits in
        # /opt/flink/lib on every node (see flink/Dockerfile), and shipping a
        # second copy per job risks the same classes being loaded twice.
        jar_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "lib", "flink-sql-connector-kafka-5.0.0-2.2.jar",
        )
        t_env.get_config().get_configuration().set_string(
            "pipeline.jars", f"file://{jar_path}"
        )

        # The embedded MiniCluster's internal components (JobManager RPC,
        # BlobServer, TaskManager data exchange) resolve this Mac's hostname to
        # a LAN IP by default — and that IP can change between the components
        # binding and connecting to each other, causing "Could not connect to
        # BlobServer" timeouts. Forcing everything to loopback avoids relying
        # on that resolution at all.
        #
        # None of this applies to a real cluster, where these addresses are how
        # the JobManager and TaskManagers find each other across containers —
        # pinning them to loopback there would stop them connecting at all.
        for key in ("rest.bind-address", "rest.address",
                    "jobmanager.bind-host", "jobmanager.rpc.address",
                    "taskmanager.bind-host", "taskmanager.host"):
            t_env.get_config().get_configuration().set_string(key, "localhost")

        # Without an explicit port the MiniCluster binds REST to an ephemeral
        # one, because MiniClusterConfiguration defaults the bind port to 0 so
        # that several local jobs can coexist. That makes the web UI land on a
        # different port every run, so pin it.
        t_env.get_config().get_configuration().set_string("rest.port", "8081")

    consume_login_events(t_env)


