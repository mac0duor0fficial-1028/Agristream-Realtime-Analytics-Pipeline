"""Pure-Python agricultural sensor simulator (no Kafka dependency, unit-testable)."""
from __future__ import annotations

import json
import math
import os
import random
from dataclasses import dataclass


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


@dataclass(frozen=True)
class SimConfig:
    num_sensors: int = 30
    num_fields: int = 5
    late_ratio: float = 0.03          # share of events delivered late
    max_lateness_ms: int = 8000       # must stay < Flink watermark delay to avoid drops
    malformed_ratio: float = 0.005    # share of deliberately bad events (-> DLQ)
    seed: int | None = None

    @staticmethod
    def from_env() -> SimConfig:
        e = os.environ
        seed = e.get("SEED")
        return SimConfig(
            num_sensors=int(e.get("NUM_SENSORS", "30")),
            num_fields=int(e.get("NUM_FIELDS", "5")),
            late_ratio=float(e.get("LATE_RATIO", "0.03")),
            max_lateness_ms=int(e.get("MAX_LATENESS_MS", "8000")),
            malformed_ratio=float(e.get("MALFORMED_RATIO", "0.005")),
            seed=int(seed) if seed else None,
        )


MALFORMED_KINDS = ("not_json", "truncated", "missing_field", "out_of_range", "wrong_type")


class Fleet:
    """A fleet of sensors spread across fields; each field has slowly drifting conditions."""

    def __init__(self, cfg: SimConfig):
        if cfg.num_sensors < 1 or cfg.num_fields < 1:
            raise ValueError("num_sensors and num_fields must be >= 1")
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.sensors = [(f"S-{i:03d}", f"F-{i % cfg.num_fields:02d}") for i in range(cfg.num_sensors)]
        self._field_moisture = {
            f"F-{i:02d}": self.rng.uniform(22.0, 38.0) for i in range(cfg.num_fields)
        }

    def _drift(self, field_id: str) -> float:
        m = self._field_moisture[field_id] + self.rng.gauss(0, 0.05)
        self._field_moisture[field_id] = _clip(m, 12.0, 55.0)
        return self._field_moisture[field_id]

    def make_event(self, sensor_id: str, field_id: str, now_ms: int) -> dict:
        r = self.rng
        hour = (now_ms / 3_600_000) % 24
        diurnal = math.sin((hour - 9) / 24 * 2 * math.pi)  # warmest mid-afternoon
        skew = r.randint(1, self.cfg.max_lateness_ms) if r.random() < self.cfg.late_ratio else 0
        return {
            "sensor_id": sensor_id,
            "field_id": field_id,
            "soil_moisture": round(_clip(r.gauss(self._drift(field_id), 1.2), 1, 99), 2),
            "soil_temp_c": round(_clip(r.gauss(20 + 4 * diurnal, 0.8), -10, 50), 2),
            "air_temp_c": round(_clip(r.gauss(24 + 7 * diurnal, 1.2), -20, 50), 2),
            "humidity": round(_clip(r.gauss(65 - 15 * diurnal, 4), 5, 99), 2),
            "ph": round(_clip(r.gauss(6.6, 0.15), 3.5, 9.5), 2),
            "event_ts": now_ms - skew,
        }

    def corrupt(self, event: dict) -> bytes:
        """Return a deliberately bad payload for DLQ testing."""
        kind = self.rng.choice(MALFORMED_KINDS)
        if kind == "not_json":
            return b"sensor-firmware-panic: 0xDEADBEEF"
        if kind == "truncated":
            return json.dumps(event).encode()[: self.rng.randint(5, 40)]
        bad = dict(event)
        if kind == "missing_field":
            bad.pop("field_id", None)
        elif kind == "out_of_range":
            bad["soil_moisture"] = 250.0
        elif kind == "wrong_type":
            bad["ph"] = "acidic"
        return json.dumps(bad).encode()

    def next_message(self, now_ms: int) -> tuple[str, bytes]:
        """Returns (kafka_key, kafka_value)."""
        sensor_id, field_id = self.rng.choice(self.sensors)
        event = self.make_event(sensor_id, field_id, now_ms)
        if self.rng.random() < self.cfg.malformed_ratio:
            return sensor_id, self.corrupt(event)
        return sensor_id, json.dumps(event, separators=(",", ":")).encode()
