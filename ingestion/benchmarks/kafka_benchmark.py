"""Benchmark Kafka-to-PostgreSQL without deleting data or stopping services.

Examples:
  python -m ingestion.benchmarks.kafka_benchmark --sizes 100 1000 5000
  python -m ingestion.benchmarks.kafka_benchmark --sizes 100 1000 5000 --continuous-seconds 300 --continuous-rate 20
"""
from __future__ import annotations

import argparse
import json
import math
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaProducer

from ingestion.src import streaming
from ingestion.src.database import get_connection
from ingestion.src.streaming import EventKeySerializer, JsonSerializer, ensure_topic, internal_ids, make_event
from ingestion.src.config import PROJECT_ROOT, settings


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(p * len(ordered)) - 1)], 3)


def metrics_for(run_id: str, group_id: str, topic: str, consumer_timing: dict[str, float]) -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT processing_latency_ms FROM simulated_events WHERE payload->>'benchmark_run_id'=%s", (run_id,))
            event_age_ms = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
            cur.execute(
                "SELECT COALESCE(sum(received_count),0),COALESCE(sum(inserted_count),0),"
                "COALESCE(sum(duplicate_count),0),COALESCE(sum(failed_count),0),"
                "array_agg(processing_duration_ms ORDER BY batch_id) "
                "FROM streaming_consumer_batches WHERE consumer_group=%s AND topic=%s",
                (group_id, topic),
            )
            received, inserted, duplicates, failed, durations = cur.fetchone()
    durations_ms = [float(x) for x in (durations or [])]
    span_s = max(0.001, consumer_timing.get("elapsed_seconds", 0.0))
    return {
        "events_consumed": int(received), "events_inserted": int(inserted),
        "duplicate_events_suppressed": int(duplicates), "failed_events": int(failed),
        "consumer_batch_span_seconds": round(span_s, 3),
        "consumer_events_per_second": round(received / span_s, 3) if span_s else 0,
        "consumer_rate_clock": "time.perf_counter from first poll result through last PostgreSQL commit",
        "consumer_batch_processing_ms": {
            "p50": percentile(durations_ms, .50), "p95": percentile(durations_ms, .95),
            "p99": percentile(durations_ms, .99), "mean": round(sum(durations_ms) / len(durations_ms), 3) if durations_ms else None,
            "samples": len(durations_ms),
        },
        "event_to_postgres_latency_ms": {
            "p50": percentile(event_age_ms, .50), "p95": percentile(event_age_ms, .95),
            "p99": percentile(event_age_ms, .99),
            "mean": round(sum(event_age_ms) / len(event_age_ms), 3) if event_age_ms else None,
            "samples": len(event_age_ms),
        },
    }


def count_persisted(run_id: str) -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM simulated_events WHERE payload->>'benchmark_run_id'=%s", (run_id,))
            return int(cur.fetchone()[0])


def run_case(label: str, event_count: int | None, duration_seconds: int | None,
             rate_per_second: float | None) -> dict[str, Any]:
    run_id = uuid.uuid4().hex
    topic = f"ecommerce-bench-{run_id[:16]}"
    group_id = f"benchmark-{run_id[:16]}"
    ensure_topic(topic)
    customers, products = internal_ids()
    streaming.stop_event.clear()
    consumer_errors: list[str] = []
    consumer_timing: dict[str, float] = {}

    def consume() -> None:
        try:
            streaming.consume(None, group_id, topic, consumer_timing)
        except Exception as exc:  # retained as run evidence for the caller
            consumer_errors.append(f"{type(exc).__name__}: {exc}")

    consumer_thread = threading.Thread(target=consume, name=f"benchmark-consumer-{label}", daemon=True)
    consumer_thread.start()
    time.sleep(1.0)  # allow the topic assignment to settle before publishing

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, acks="all", retries=5,
        max_in_flight_requests_per_connection=1, value_serializer=JsonSerializer(),
        key_serializer=EventKeySerializer(),
    )
    futures = []
    produced_started = time.perf_counter()
    next_progress = 60
    seq = 0
    try:
        while (event_count is not None and seq < event_count) or (
            duration_seconds is not None and time.perf_counter() - produced_started < duration_seconds
        ):
            event = make_event(customers, products)
            event["payload"]["benchmark_run_id"] = run_id
            event["payload"]["benchmark_case"] = label
            futures.append(producer.send(topic, key=event["event_id"], value=event))
            seq += 1
            if rate_per_second:
                scheduled = produced_started + seq / rate_per_second
                time.sleep(max(0, scheduled - time.perf_counter()))
            if duration_seconds and time.perf_counter() - produced_started >= next_progress:
                print(f"{label}: elapsed about {next_progress}s, sent {seq}", flush=True)
                next_progress += 60
        producer.flush(timeout=60)
    finally:
        producer.close(timeout=30)
    producer_elapsed = max(0.001, time.perf_counter() - produced_started)
    acked = 0
    producer_errors = 0
    for future in futures:
        try:
            future.get(timeout=30)
            acked += 1
        except Exception as exc:
            producer_errors += 1
            print(f"{label}: produce failure: {type(exc).__name__}: {exc}", flush=True)

    wait_started = time.perf_counter()
    timeout_at = wait_started + max(120, (duration_seconds or 0) + 120)
    while count_persisted(run_id) < acked and time.perf_counter() < timeout_at and not consumer_errors:
        time.sleep(1)
    persisted = count_persisted(run_id)
    streaming.stop_event.set()
    consumer_thread.join(timeout=15)
    streaming.stop_event.clear()
    if consumer_thread.is_alive():
        consumer_errors.append("Consumer thread did not stop within 15 seconds")

    data = metrics_for(run_id, group_id, topic, consumer_timing)
    data.update({
        "case": label, "benchmark_run_id": run_id, "topic": topic, "consumer_group": group_id,
        "requested_events": event_count, "continuous_duration_seconds": duration_seconds,
        "continuous_target_events_per_second": rate_per_second,
        "events_produced": acked, "producer_failures": producer_errors,
        "producer_elapsed_seconds": round(producer_elapsed, 3),
        "producer_events_per_second": round(acked / producer_elapsed, 3),
        "persisted_events_for_run": persisted,
        "end_to_end_events_per_second": round(acked / max(.001, time.perf_counter() - produced_started), 3),
        "consumer_errors": consumer_errors,
        "status": "passed" if acked == persisted and data["events_consumed"] == acked and not producer_errors and not consumer_errors and data["failed_events"] == 0 else "failed",
    })
    print(json.dumps(data, indent=2), flush=True)
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[100, 1000, 5000])
    parser.add_argument("--continuous-seconds", type=int, default=300)
    parser.add_argument("--continuous-rate", type=float, default=20.0, help="paced continuous events per second")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "docs" / "performance_benchmark_results.json")
    args = parser.parse_args()
    if any(n <= 0 for n in args.sizes) or args.continuous_seconds <= 0 or args.continuous_rate <= 0:
        parser.error("sizes, duration, and event rate must be positive")
    results = []
    for size in args.sizes:
        results.append(run_case(f"burst-{size}", size, None, None))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({"results": results}, indent=2) + "\n", encoding="utf-8")
    results.append(run_case("continuous-5m", None, args.continuous_seconds, args.continuous_rate))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"results": results}, indent=2) + "\n", encoding="utf-8")
    if any(r["status"] != "passed" for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
