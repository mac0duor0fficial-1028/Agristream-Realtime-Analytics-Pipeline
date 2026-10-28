import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "producer"))
from sensor_sim import Fleet, SimConfig  # noqa: E402

NOW = 1_761_550_000_000
REQUIRED = {
    "sensor_id", "field_id", "soil_moisture", "soil_temp_c", "air_temp_c", "humidity", "ph", "event_ts",
}


def fleet(**kw):
    return Fleet(SimConfig(seed=42, malformed_ratio=0.0, **kw))


def test_event_has_contract_fields():
    f = fleet()
    ev = f.make_event("S-001", "F-01", NOW)
    assert set(ev) == REQUIRED
    assert isinstance(ev["event_ts"], int)


def test_values_always_within_validation_ranges():
    f = fleet()
    for _ in range(5000):
        _, raw = f.next_message(NOW)
        ev = json.loads(raw)
        assert 0 <= ev["soil_moisture"] <= 100
        assert -20 <= ev["soil_temp_c"] <= 60
        assert -40 <= ev["air_temp_c"] <= 60
        assert 0 <= ev["humidity"] <= 100
        assert 0 <= ev["ph"] <= 14


def test_lateness_bounded_and_present():
    f = fleet(late_ratio=0.5, max_lateness_ms=8000)
    skews = [NOW - f.make_event("S-001", "F-01", NOW)["event_ts"] for _ in range(2000)]
    assert max(skews) <= 8000 and min(skews) >= 0
    assert any(s > 0 for s in skews)


def test_no_lateness_when_disabled():
    f = fleet(late_ratio=0.0)
    assert all(f.make_event("S-001", "F-01", NOW)["event_ts"] == NOW for _ in range(200))


def test_sensor_to_field_mapping_is_stable_and_partition_key_is_sensor():
    f = fleet(num_sensors=12, num_fields=3)
    mapping = dict(f.sensors)
    assert len(mapping) == 12 and set(mapping.values()) == {"F-00", "F-01", "F-02"}
    for _ in range(200):
        key, raw = f.next_message(NOW)
        ev = json.loads(raw)
        assert key == ev["sensor_id"] and mapping[key] == ev["field_id"]


def test_malformed_events_are_invalid_for_pipeline():
    f = Fleet(SimConfig(seed=1, malformed_ratio=1.0))
    bad = 0
    for _ in range(500):
        _, raw = f.next_message(NOW)
        try:
            ev = json.loads(raw)
            ok = (REQUIRED <= set(ev) and isinstance(ev["ph"], (int, float))
                  and 0 <= ev["soil_moisture"] <= 100)
        except ValueError:
            ok = False
        bad += (not ok)
    assert bad == 500


def test_seed_is_deterministic():
    a, b = Fleet(SimConfig(seed=7)), Fleet(SimConfig(seed=7))
    assert [a.next_message(NOW) for _ in range(50)] == [b.next_message(NOW) for _ in range(50)]


def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        Fleet(SimConfig(num_sensors=0))
