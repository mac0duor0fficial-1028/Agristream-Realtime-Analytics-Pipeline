"""
Agristream - PyFlink streaming job.

    Kafka (sensor.readings, raw JSON)
        -> parse + validate (SQL)           -> invalid events -> Kafka DLQ topic
        -> 1-minute event-time tumbling windows per field
        -> PostgreSQL field_metrics_1m (idempotent upsert on field_id, window_start)

Design notes
  * The source is read with the `raw` format and parsed in SQL, so a malformed
    message can never crash the job (a poison pill) - it is routed to the DLQ.
  * Time semantics: event time (sensor timestamp) + bounded-out-of-orderness watermark.
  * Sink semantics: at-least-once delivery + primary-key upsert = effectively-once results.
  * Pure SQL (no Python UDFs) means TaskManagers execute only JVM code.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def sql_str(value: str) -> str:
    """Escape a Python string as a Flink SQL string literal body."""
    return str(value).replace("'", "''")


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "kafka:9092"
    source_topic: str = "sensor.readings"
    dlq_topic: str = "sensor.readings.dlq"
    consumer_group: str = "agristream-flink"
    pg_host: str = "postgres"
    pg_port: int = 5432
    pg_db: str = "agristream"
    pg_user: str = ""
    pg_password: str = ""
    watermark_delay_s: int = 10
    max_future_skew_min: int = 5
    source_idle_timeout: str = "30 s"
    pipeline_name: str = "agristream-aggregate"

    @staticmethod
    def from_env() -> Settings:
        env = os.environ
        missing = [k for k in ("PG_USER", "PG_PASSWORD") if not env.get(k)]
        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
        return Settings(
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            source_topic=env.get("SOURCE_TOPIC", "sensor.readings"),
            dlq_topic=env.get("DLQ_TOPIC", "sensor.readings.dlq"),
            consumer_group=env.get("CONSUMER_GROUP", "agristream-flink"),
            pg_host=env.get("PG_HOST", "postgres"),
            pg_port=int(env.get("PG_PORT", "5432")),
            pg_db=env.get("PG_DB", "agristream"),
            pg_user=env["PG_USER"],
            pg_password=env["PG_PASSWORD"],
            watermark_delay_s=int(env.get("WATERMARK_DELAY_SECONDS", "10")),
            max_future_skew_min=int(env.get("MAX_FUTURE_SKEW_MINUTES", "5")),
            source_idle_timeout=env.get("SOURCE_IDLE_TIMEOUT", "30 s"),
            pipeline_name=env.get("PIPELINE_NAME", "agristream-aggregate"),
        )


# ----------------------------------------------------------------------------
# SQL builders (pure functions - unit-tested without Flink)
# ----------------------------------------------------------------------------
def _json_field(path: str, cast: str | None = None) -> str:
    expr = f"JSON_VALUE(raw, '$.{path}')"
    return f"TRY_CAST({expr} AS {cast})" if cast else expr


def source_ddl(s: Settings, with_options: dict[str, str] | None = None) -> str:
    """Raw Kafka source. Computed columns parse JSON; WATERMARK defines event time."""
    if with_options is None:
        with_options = {
            "connector": "kafka",
            "topic": s.source_topic,
            "properties.bootstrap.servers": s.kafka_bootstrap,
            "properties.group.id": s.consumer_group,
            # Resume from committed offsets; start at earliest on first run.
            "scan.startup.mode": "group-offsets",
            "properties.auto.offset.reset": "earliest",
            "scan.topic-partition-discovery.interval": "60 s",
            "format": "raw",
        }
    opts = ",\n    ".join(f"'{sql_str(k)}' = '{sql_str(v)}'" for k, v in with_options.items())
    event_ts = _json_field("event_ts", "BIGINT")
    return f"""
CREATE TABLE raw_readings (
    raw           STRING,
    kafka_ts      TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    sensor_id     AS {_json_field('sensor_id')},
    field_id      AS {_json_field('field_id')},
    soil_moisture AS {_json_field('soil_moisture', 'DOUBLE')},
    soil_temp_c   AS {_json_field('soil_temp_c', 'DOUBLE')},
    air_temp_c    AS {_json_field('air_temp_c', 'DOUBLE')},
    humidity      AS {_json_field('humidity', 'DOUBLE')},
    ph            AS {_json_field('ph', 'DOUBLE')},
    ts            AS TO_TIMESTAMP_LTZ({event_ts}, 3),
    WATERMARK FOR ts AS ts - INTERVAL '{int(s.watermark_delay_s)}' SECOND
) WITH (
    {opts}
)"""


def classified_view_ddl(s: Settings) -> str:
    """Tags every record with a validation status. Order of WHENs = precedence."""
    return f"""
CREATE TEMPORARY VIEW classified AS
SELECT
    raw, kafka_ts, sensor_id, field_id,
    soil_moisture, soil_temp_c, air_temp_c, humidity, ph, ts,
    CASE
        WHEN raw IS NULL OR NOT (raw IS JSON OBJECT) THEN 'invalid_json'
        WHEN sensor_id IS NULL OR field_id IS NULL OR ts IS NULL
          OR soil_moisture IS NULL OR soil_temp_c IS NULL OR air_temp_c IS NULL
          OR humidity IS NULL OR ph IS NULL THEN 'missing_or_malformed_field'
        WHEN NOT (soil_moisture BETWEEN 0 AND 100
              AND soil_temp_c   BETWEEN -20 AND 60
              AND air_temp_c    BETWEEN -40 AND 60
              AND humidity      BETWEEN 0 AND 100
              AND ph            BETWEEN 0 AND 14) THEN 'out_of_range'
        -- A far-future timestamp would drag the watermark forward and make
        -- every later (correct) event look late. Quarantine it instead.
        WHEN ts > kafka_ts + INTERVAL '{int(s.max_future_skew_min)}' MINUTE THEN 'future_timestamp'
        ELSE 'ok'
    END AS status
FROM raw_readings"""


VALID_VIEW_DDL = """
CREATE TEMPORARY VIEW valid_readings AS
SELECT sensor_id, field_id, soil_moisture, soil_temp_c, air_temp_c, humidity, ph, ts
FROM classified
WHERE status = 'ok'"""


def dlq_sink_ddl(s: Settings, with_options: dict[str, str] | None = None) -> str:
    if with_options is None:
        with_options = {
            "connector": "kafka",
            "topic": s.dlq_topic,
            "properties.bootstrap.servers": s.kafka_bootstrap,
            "format": "raw",
            "sink.delivery-guarantee": "at-least-once",
        }
    opts = ",\n    ".join(f"'{sql_str(k)}' = '{sql_str(v)}'" for k, v in with_options.items())
    return f"""
CREATE TABLE dlq_sink (
    message STRING
) WITH (
    {opts}
)"""


def metrics_sink_ddl(s: Settings, with_options: dict[str, str] | None = None) -> str:
    if with_options is None:
        with_options = {
            "connector": "jdbc",
            "url": f"jdbc:postgresql://{s.pg_host}:{s.pg_port}/{s.pg_db}",
            "table-name": "field_metrics_1m",
            "username": s.pg_user,
            "password": s.pg_password,
            "sink.buffer-flush.max-rows": "500",
            "sink.buffer-flush.interval": "2 s",
            "sink.max-retries": "5",
        }
    opts = ",\n    ".join(f"'{sql_str(k)}' = '{sql_str(v)}'" for k, v in with_options.items())
    return f"""
CREATE TABLE field_metrics_1m (
    field_id          STRING,
    window_start      TIMESTAMP(3),
    window_end        TIMESTAMP(3),
    reading_count     BIGINT,
    avg_soil_moisture DOUBLE,
    min_soil_moisture DOUBLE,
    max_soil_moisture DOUBLE,
    avg_soil_temp_c   DOUBLE,
    avg_air_temp_c    DOUBLE,
    avg_humidity      DOUBLE,
    avg_ph            DOUBLE,
    PRIMARY KEY (field_id, window_start) NOT ENFORCED
) WITH (
    {opts}
)"""


AGGREGATE_INSERT = """
INSERT INTO field_metrics_1m
SELECT
    field_id,
    CAST(window_start AS TIMESTAMP(3)),
    CAST(window_end   AS TIMESTAMP(3)),
    COUNT(*),
    AVG(soil_moisture), MIN(soil_moisture), MAX(soil_moisture),
    AVG(soil_temp_c), AVG(air_temp_c), AVG(humidity), AVG(ph)
FROM TABLE(
    TUMBLE(TABLE valid_readings, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY field_id, window_start, window_end"""


DLQ_INSERT = """
INSERT INTO dlq_sink
SELECT JSON_OBJECT(
    'reason'          VALUE status,
    'payload'         VALUE raw,
    'kafka_timestamp' VALUE CAST(kafka_ts AS STRING)
)
FROM classified
WHERE status <> 'ok'"""


def build_statements(s: Settings) -> list[str]:
    """DDL statements in execution order (inserts are submitted separately)."""
    return [
        source_ddl(s),
        classified_view_ddl(s),
        VALID_VIEW_DDL,
        metrics_sink_ddl(s),
        dlq_sink_ddl(s),
    ]


def main() -> None:
    from pyflink.table import EnvironmentSettings, TableEnvironment  # lazy: keeps tests light

    s = Settings.from_env()
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("pipeline.name", s.pipeline_name)
    cfg.set("table.local-time-zone", "UTC")
    # Idle partitions must not hold back the watermark.
    cfg.set("table.exec.source.idle-timeout", s.source_idle_timeout)

    for stmt in build_statements(s):
        t_env.execute_sql(stmt)

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(AGGREGATE_INSERT)
    stmt_set.add_insert_sql(DLQ_INSERT)
    # With `flink run -d` this returns right after submission.
    stmt_set.execute()


if __name__ == "__main__":
    main()
