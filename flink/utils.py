from sqlalchemy import Numeric, String, DateTime, CHAR, BigInteger, Integer
from sqlmodel import SQLModel

FLINK_TYPE_MAP = {
    String: lambda col: f"VARCHAR({col.type.length})" if col.type.length else "STRING",
    CHAR: lambda col: f"CHAR({col.type.length})",
    # Debezium's decimal.handling.mode=string emits NUMERIC columns as plain
    # JSON strings (e.g. "500.00"), not JSON numbers — STRING here, not
    # DECIMAL, since the plain 'json' format (unlike 'debezium-json') has no
    # special-cased decoding and would otherwise fail to parse the whole row.
    Numeric: lambda col: "STRING",
    # Postgres timestamptz / Debezium's ZonedTimestamp emit microsecond
    # precision (6 fractional digits), but a Flink watermark column must be
    # TIMESTAMP(p) with p <= 3 — and its JSON parser silently nulls a field
    # it can't match to the declared precision rather than failing the row.
    # Kept as STRING here; _watermark_timestamp_expr() truncates and CASTs
    # it into a proper TIMESTAMP(3) computed column instead.
    DateTime: lambda col: "STRING",
    BigInteger: lambda col: "BIGINT",
    Integer: lambda col: "INT",
}

def _interval_literal(duration: str) -> str:
    """
    Flink/Calcite interval syntax needs the amount and unit as separate
    tokens — INTERVAL '1' SECOND, not INTERVAL '1 SECOND' — and the unit
    keyword is singular (SECOND, not SECONDS). Takes the friendlier "1
    SECOND" / "30 MINUTE" form and reshapes it correctly.
    """
    amount, unit = duration.split()
    return f"INTERVAL '{amount}' {unit.rstrip('Ss').upper()}"


def _watermark_timestamp_expr(raw_col_ref: str) -> str:
    """
    Truncates a Debezium ISO-8601 timestamp string (microsecond precision,
    'T' separator, trailing 'Z') down to millisecond precision and CASTs it
    to TIMESTAMP(3) — CAST is lenient about this in a way the JSON
    deserializer's strict type coercion is not.
    """
    return f"CAST(SUBSTRING(REPLACE({raw_col_ref}, 'T', ' '), 1, 23) AS TIMESTAMP(3))"


def _flink_fields(model: type[SQLModel]) -> list[str]:
    cols = []
    for col in model.__table__.columns:
        for sa_type, fmt in FLINK_TYPE_MAP.items():
            if isinstance(col.type, sa_type):
                cols.append(f"{col.name} {fmt(col)}")
                break
        else:
            cols.append(f"{col.name} STRING")  # fallback, e.g. UUID columns
    return cols


def debezium_table_ddl(model: type[SQLModel], *, topic: str,
                        watermark_col: str, watermark_lag: str = "1 SECOND",
                        bootstrap_servers: str, group_id: str) -> str:
    """
    Declares the source as current-state rows via the 'debezium-json' format,
    which collapses the Debezium envelope into a changelog stream (+I/-U/+U/-D).
    Use this when you want the table's current state, not when you need to
    filter by operation type — RowKind isn't filterable in a WHERE clause.
    """
    table = model.__table__
    col_list = ",\n    ".join(_flink_fields(model))
    ts_col = f"{watermark_col}_ts"
    return f"""CREATE TABLE {table.name} (
    {col_list},
    {ts_col} AS {_watermark_timestamp_expr(watermark_col)},
    WATERMARK FOR {ts_col} AS {ts_col} - {_interval_literal(watermark_lag)}
    ) WITH (
    'connector' = 'kafka',
    'topic' = '{topic}',
    'properties.bootstrap.servers' = '{bootstrap_servers}',
    'properties.group.id' = '{group_id}',
    'format' = 'debezium-json',
    'scan.startup.mode' = 'earliest-offset', -- without this, a fresh consumer group with
    -- no committed offsets and no reset policy makes Kafka throw NoOffsetForPartitionException
    'scan.watermark.idle-timeout' = '5000' --  Adding idle-timeout detection to the sources
    -- since Flink's event-time watermark can stall — the interval join holds results in state until 
    -- the watermark passes the join's upper bound, and an idle Kafka partition (no new messages) never advances it. 
    )"""


def debezium_envelope_ddl(model: type[SQLModel], *, topic: str,
                           watermark_col: str, watermark_lag: str = "1 SECOND",
                           bootstrap_servers: str, group_id: str) -> str:
    """
    Declares the source over the raw Debezium JSON envelope (before/after/op)
    instead of letting 'debezium-json' collapse it into a changelog stream.
    Needed whenever a query must filter on operation type, e.g.:

        SELECT after.* FROM transactions WHERE op = 'c'   -- creates only

    'c' = create, 'u' = update, 'd' = delete, 'r' = initial snapshot read.
    The watermark is declared on a computed column pulled out of `after`
    since a WATERMARK FOR clause can't reference a nested field directly.
    """
    row_fields = ",\n        ".join(_flink_fields(model))
    ts_col = f"{watermark_col}_ts"
    return f"""CREATE TABLE {model.__table__.name} (
    before ROW<{row_fields}>,
    after ROW<{row_fields}>,
    op STRING,
    ts_ms BIGINT,
    {ts_col} AS {_watermark_timestamp_expr(f"after.{watermark_col}")},
    WATERMARK FOR {ts_col} AS {ts_col} - {_interval_literal(watermark_lag)}
    ) WITH (
    'connector' = 'kafka',
    'topic' = '{topic}',
    'properties.bootstrap.servers' = '{bootstrap_servers}',
    'properties.group.id' = '{group_id}',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'earliest-offset',
    'scan.watermark.idle-timeout' = '5000'
    )"""