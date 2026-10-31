"""Measure PostgreSQL query/insert behavior without changing existing data."""
from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from psycopg2.extras import execute_values

from ingestion.src.config import PROJECT_ROOT
from ingestion.src.database import get_connection


BASELINE = PROJECT_ROOT / "docs" / "postgres_benchmark_baseline.json"
OUTPUT = PROJECT_ROOT / "docs" / "postgres_benchmark_results.json"


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(p * len(ordered)) - 1)], 3)


def snapshot() -> dict:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            database_bytes = int(cur.fetchone()[0])
            cur.execute("SELECT pg_total_relation_size('simulated_events'::regclass),count(*) FROM simulated_events")
            events_bytes, event_rows = cur.fetchone()
            cur.execute("SELECT pg_total_relation_size('api_products'::regclass),count(*) FROM api_products")
            api_bytes, api_rows = cur.fetchone()
    return {"captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "database_size_bytes": database_bytes, "simulated_events_size_bytes": int(events_bytes),
            "simulated_event_rows": int(event_rows), "api_products_size_bytes": int(api_bytes),
            "api_product_rows": int(api_rows)}


def capture_baseline() -> None:
    value = snapshot()
    BASELINE.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(value, indent=2))


def index_plan(cur, name: str, sql: str) -> dict:
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
    plan = cur.fetchone()[0][0]
    nodes = []

    def visit(node):
        nodes.append({"node_type": node.get("Node Type"), "index_name": node.get("Index Name"),
                      "actual_rows": node.get("Actual Rows"), "actual_total_time_ms": node.get("Actual Total Time"),
                      "shared_hit_blocks": node.get("Shared Hit Blocks"), "shared_read_blocks": node.get("Shared Read Blocks")})
        for child in node.get("Plans", []):
            visit(child)

    visit(plan["Plan"])
    return {"name": name, "execution_time_ms": round(float(plan["Execution Time"]), 3),
            "planning_time_ms": round(float(plan["Planning Time"]), 3), "nodes": nodes}


def query_timing(sql: str, repetitions: int = 30) -> dict:
    samples = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _ in range(repetitions):
                started = time.perf_counter()
                cur.execute(sql)
                cur.fetchall()
                samples.append((time.perf_counter() - started) * 1000)
    return {"samples": repetitions, "p50_ms": percentile(samples, .5), "p95_ms": percentile(samples, .95),
            "p99_ms": percentile(samples, .99), "mean_ms": round(sum(samples) / len(samples), 3)}


def temp_insert_benchmark(rows: int = 10000, batch_size: int = 1000) -> dict:
    run_id = str(uuid4())
    samples = [(i, datetime.now(timezone.utc), f"sample-{run_id}-{i}") for i in range(rows)]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE benchmark_insert_probe (id BIGINT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL, payload TEXT NOT NULL) ON COMMIT PRESERVE ROWS")
            started = time.perf_counter()
            for offset in range(0, rows, batch_size):
                execute_values(cur, "INSERT INTO benchmark_insert_probe (id,created_at,payload) VALUES %s", samples[offset:offset + batch_size], page_size=batch_size)
            cur.execute("SELECT count(*) FROM benchmark_insert_probe")
            inserted = int(cur.fetchone()[0])
        conn.commit()
    elapsed = max(.001, time.perf_counter() - started)
    return {"rows_inserted": inserted, "elapsed_seconds": round(elapsed, 3),
            "insert_rows_per_second": round(inserted / elapsed, 3), "target": "session-local temporary table",
            "persistent_tables_modified": False}


def run_measurements() -> None:
    before = json.loads(BASELINE.read_text(encoding="utf-8")) if BASELINE.exists() else None
    queries = {
        "simulated_events_recent_by_time": "SELECT event_type,count(*) FROM simulated_events WHERE consumed_at >= now()-interval '24 hours' GROUP BY event_type",
        "simulated_events_type_and_time": "SELECT count(*) FROM simulated_events WHERE event_type='product_viewed' AND consumed_at >= now()-interval '7 days'",
        "daily_synthetic_revenue": "SELECT date_trunc('day',p.created_at),sum(p.amount) FROM payments p JOIN orders o USING(order_id) WHERE lower(p.payment_status) IN ('success','succeeded','completed') GROUP BY 1 ORDER BY 1",
    }
    explain = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for name, sql in queries.items():
                explain.append(index_plan(cur, name, sql))
    timings = {name: query_timing(sql) for name, sql in queries.items()}
    inserts = temp_insert_benchmark()
    after = snapshot()
    growth = None
    if before:
        growth = {
            "from_utc": before["captured_at_utc"], "to_utc": after["captured_at_utc"],
            "database_size_delta_bytes": after["database_size_bytes"] - before["database_size_bytes"],
            "simulated_events_size_delta_bytes": after["simulated_events_size_bytes"] - before["simulated_events_size_bytes"],
            "simulated_event_row_delta": after["simulated_event_rows"] - before["simulated_event_rows"],
            "api_products_size_delta_bytes": after["api_products_size_bytes"] - before["api_products_size_bytes"],
        }
    result = {"captured_at_utc": datetime.now(timezone.utc).isoformat(), "baseline": before,
              "current_snapshot": after, "growth_since_baseline": growth,
              "temporary_insert_benchmark": inserts, "query_wall_clock_timings_ms": timings,
              "explain_analyze": explain}
    OUTPUT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--capture-baseline", action="store_true")
    group.add_argument("--measure", action="store_true")
    args = parser.parse_args()
    capture_baseline() if args.capture_baseline else run_measurements()


if __name__ == "__main__":
    main()
