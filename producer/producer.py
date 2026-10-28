"""Kafka producer for the simulated sensor fleet."""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time

from confluent_kafka import KafkaException, Producer
from sensor_sim import Fleet, SimConfig

log = logging.getLogger("producer")

stats = {"delivered": 0, "failed": 0}


def on_delivery(err, _msg) -> None:
    if err is not None:
        stats["failed"] += 1
        log.error("delivery failed: %s", err)
    else:
        stats["delivered"] += 1


def wait_for_broker(producer: Producer, topic: str, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            md = producer.list_topics(topic, timeout=10)
            if topic in md.topics and md.topics[topic].error is None:
                log.info("connected; topic %s has %d partitions", topic, len(md.topics[topic].partitions))
                return
            log.warning("topic %s not ready yet", topic)
        except KafkaException as exc:
            log.warning("broker not ready: %s", exc)
        stop.wait(3)


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    bootstrap = os.getenv("BOOTSTRAP", "localhost:29092")
    topic = os.getenv("TOPIC", "sensor.readings")
    rate = float(os.getenv("RATE_PER_SEC", "50"))
    cfg = SimConfig.from_env()
    fleet = Fleet(cfg)

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "agristream-sim",
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
        "delivery.timeout.ms": 60000,
    })

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    wait_for_broker(producer, topic, stop)
    log.info("producing ~%.0f msg/s from %d sensors in %d fields",
             rate, cfg.num_sensors, cfg.num_fields)

    tick = 0.1
    carry = 0.0
    next_tick = time.monotonic()
    last_report = time.monotonic()
    while not stop.is_set():
        carry += rate * tick
        n, carry = int(carry), carry - int(carry)
        now_ms = int(time.time() * 1000)
        for _ in range(n):
            key, value = fleet.next_message(now_ms)
            while True:
                try:
                    producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                    break
                except BufferError:  # local queue full -> back-pressure
                    producer.poll(0.5)
        producer.poll(0)

        if time.monotonic() - last_report >= 10:
            log.info("delivered=%d failed=%d", stats["delivered"], stats["failed"])
            last_report = time.monotonic()

        next_tick += tick
        stop.wait(max(0.0, next_tick - time.monotonic()))

    log.info("shutting down, flushing ...")
    remaining = producer.flush(15)
    log.info("final delivered=%d failed=%d unsent=%d", stats["delivered"], stats["failed"], remaining)
    return 0 if remaining == 0 and stats["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
