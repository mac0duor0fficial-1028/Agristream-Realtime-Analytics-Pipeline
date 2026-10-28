"""Structural tests for the SQL emitted by the Flink job (no Flink runtime needed)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "flink" / "jobs"))
import aggregate as job  # noqa: E402


@pytest.fixture
def s():
    return job.Settings(pg_user="u", pg_password="p'w", watermark_delay_s=10)


def test_source_defines_event_time_watermark(s):
    ddl = job.source_ddl(s)
    assert "WATERMARK FOR ts AS ts - INTERVAL '10' SECOND" in ddl
    assert "'format' = 'raw'" in ddl
    assert "'scan.startup.mode' = 'group-offsets'" in ddl


def test_sink_has_primary_key_for_upsert(s):
    assert "PRIMARY KEY (field_id, window_start) NOT ENFORCED" in job.metrics_sink_ddl(s)


def test_credentials_are_escaped(s):
    assert "'password' = 'p''w'" in job.metrics_sink_ddl(s)


def test_validation_covers_all_failure_classes(s):
    view = job.classified_view_ddl(s)
    for reason in ("invalid_json", "missing_or_malformed_field", "out_of_range", "future_timestamp"):
        assert f"'{reason}'" in view


def test_windowing_and_dlq_statements():
    assert "TUMBLE(TABLE valid_readings, DESCRIPTOR(ts), INTERVAL '1' MINUTE)" in job.AGGREGATE_INSERT
    assert "status <> 'ok'" in job.DLQ_INSERT


def test_statement_order_creates_dependencies_first(s):
    names = ["raw_readings", "classified", "valid_readings", "field_metrics_1m", "dlq_sink"]
    stmts = job.build_statements(s)
    assert [n in st for n, st in zip(names, stmts, strict=True)] == [True] * 5


def test_missing_credentials_fail_fast(monkeypatch):
    monkeypatch.delenv("PG_USER", raising=False)
    monkeypatch.delenv("PG_PASSWORD", raising=False)
    with pytest.raises(RuntimeError):
        job.Settings.from_env()
