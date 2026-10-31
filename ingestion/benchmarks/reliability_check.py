"""Safe fault checks using isolated topics and injected/endpoint failures."""
from __future__ import annotations

import argparse
import json
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg2
from kafka import KafkaProducer
from kafka.errors import KafkaError
from requests import RequestException

from ingestion.src import streaming
from ingestion.src.api_client import DummyJsonClient
from ingestion.src.config import PROJECT_ROOT, settings
from ingestion.src.database import get_connection
from ingestion.src.main import ingest_endpoint
from ingestion.src.streaming import EventKeySerializer, JsonSerializer, ensure_topic, make_event


def new_topic(prefix: str) -> tuple[str, str]:
    token = uuid.uuid4().hex[:14]
    topic = f"ecommerce-{prefix}-{token}"
    group = f"reliability-{token}"
    ensure_topic(topic)
    return topic, group


def send_events(topic: str, events: list[dict]) -> None:
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, acks="all", retries=3,
        max_in_flight_requests_per_connection=1, value_serializer=JsonSerializer(),
        key_serializer=EventKeySerializer(),
    )
    try:
        futures = [producer.send(topic, key=e["event_id"], value=e) for e in events]
        producer.flush(timeout=30)
        for f in futures:
            f.get(timeout=30)
    finally:
        producer.close(timeout=10)


def rows_for_event_ids(event_ids: list[str]) -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM simulated_events WHERE event_id = ANY(%s::uuid[])", (event_ids,))
            return int(cur.fetchone()[0])


def test_consumer_restart() -> dict:
    topic, group = new_topic("restart")
    customers, products = streaming.internal_ids()
    run_id = uuid.uuid4().hex
    events = [streaming.make_event(customers, products) for _ in range(5)]
    for e in events:
        e["payload"]["reliability_run_id"] = run_id
    send_events(topic, events)
    streaming.stop_event.clear()
    streaming.consume(2, group, topic)
    first_count = rows_for_event_ids([e["event_id"] for e in events])
    streaming.consume(3, group, topic)
    final_count = rows_for_event_ids([e["event_id"] for e in events])
    passed = first_count == 2 and final_count == 5
    return {"name": "consumer_restart_and_offset_recovery", "status": "passed" if passed else "failed",
            "produced": 5, "persisted_after_first_consumer": first_count, "persisted_after_restart": final_count,
            "topic": topic, "consumer_group": group}


def test_duplicate_delivery() -> dict:
    topic, group = new_topic("duplicate")
    event = streaming.make_event(*streaming.internal_ids())
    event["payload"]["reliability_duplicate_test"] = True
    send_events(topic, [event, dict(event)])
    streaming.stop_event.clear()
    streaming.consume(2, group, topic)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT inserted_count,duplicate_count,failed_count FROM streaming_consumer_batches WHERE consumer_group=%s AND topic=%s", (group, topic))
            inserted, duplicates, failed = cur.fetchone()
    count = rows_for_event_ids([event["event_id"]])
    passed = count == 1 and inserted == 1 and duplicates == 1 and failed == 0
    return {"name": "duplicate_event_delivery", "status": "passed" if passed else "failed",
            "messages_delivered": 2, "unique_rows": count, "inserted": inserted,
            "duplicates_suppressed": duplicates, "failed": failed, "topic": topic}


def test_injected_postgres_outage_replay() -> dict:
    topic, group = new_topic("pg-recovery")
    event = streaming.make_event(*streaming.internal_ids())
    event["payload"]["reliability_pg_injection"] = True
    send_events(topic, [event])
    original = streaming.persist_batch
    injected = {"once": True}

    def fail_once(*args, **kwargs):
        if injected["once"]:
            injected["once"] = False
            raise psycopg2.OperationalError("injected transient PostgreSQL connection outage")
        return original(*args, **kwargs)

    streaming.persist_batch = fail_once
    streaming.stop_event.clear()
    failed_as_expected = False
    try:
        streaming.consume(1, group, topic)
    except psycopg2.OperationalError:
        failed_as_expected = True
    finally:
        streaming.persist_batch = original
    before_replay = rows_for_event_ids([event["event_id"]])
    streaming.consume(1, group, topic)
    after_replay = rows_for_event_ids([event["event_id"]])
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM streaming_consumer_batches WHERE consumer_group=%s AND topic=%s AND status='failed'", (group, topic))
            failure_batches = cur.fetchone()[0]
    passed = failed_as_expected and before_replay == 0 and after_replay == 1 and failure_batches == 1
    return {"name": "injected_postgres_outage_and_kafka_replay", "status": "passed" if passed else "failed",
            "injected_connection_failure": failed_as_expected, "persisted_before_replay": before_replay,
            "persisted_after_replay": after_replay, "failure_batches_recorded": failure_batches,
            "topic": topic, "consumer_group": group,
            "scope_note": "Injected at the consumer persistence call; PostgreSQL service was not stopped."}


def test_api_timeout() -> dict:
    class SlowHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            time.sleep(.5)
            try:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"products":[],"total":0,"skip":0}')
            except (ConnectionError, BrokenPipeError):
                pass

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    from ingestion.src import api_client
    prior_url = api_client.settings.api_base_url
    object.__setattr__(api_client.settings, "api_base_url", f"http://127.0.0.1:{server.server_port}")
    client = DummyJsonClient(timeout=(.1, .1), retries=0)
    run_id = None
    timed_out = False
    try:
        try:
            ingest_endpoint(client, "products", lambda _records, _run_id: 0)
        except RequestException:
            timed_out = True
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT run_id,status,error_message FROM api_ingestion_runs WHERE entity='products' ORDER BY run_id DESC LIMIT 1")
                run_id, status, error_message = cur.fetchone()
    finally:
        client.close()
        object.__setattr__(api_client.settings, "api_base_url", prior_url)
        server.shutdown()
        server.server_close()
    passed = timed_out and status == "failed" and "Timeout" in (error_message or "")
    return {"name": "api_request_timeout", "status": "passed" if passed else "failed",
            "timeout_raised": timed_out, "ingestion_run_id": run_id,
            "ingestion_run_status": status, "error_class_recorded": error_message.split(":", 1)[0] if error_message else None}


def test_api_retry_recovers() -> dict:
    class RetryHandler(BaseHTTPRequestHandler):
        requests_seen = 0

        def do_GET(self):
            type(self).requests_seen += 1
            if type(self).requests_seen == 1:
                time.sleep(.4)
            body = b'{"products":[{"id":1}],"total":1,"skip":0}'
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (ConnectionError, BrokenPipeError):
                pass

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), RetryHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    from ingestion.src import api_client
    prior_url = api_client.settings.api_base_url
    object.__setattr__(api_client.settings, "api_base_url", f"http://127.0.0.1:{server.server_port}")
    client = DummyJsonClient(timeout=(.1, .15), retries=1)
    records = []
    error = None
    try:
        records = client.fetch_all("products")
    except RequestException as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        client.close()
        object.__setattr__(api_client.settings, "api_base_url", prior_url)
        server.shutdown()
        server.server_close()
    passed = error is None and len(records) == 1 and RetryHandler.requests_seen == 2
    return {"name": "api_retry_recovers_after_timed_out_attempt", "status": "passed" if passed else "failed",
            "attempts": RetryHandler.requests_seen, "records_returned": len(records), "error": error}


def test_unavailable_endpoints() -> list[dict]:
    # Use a currently unused local port. Neither shared service is stopped or reconfigured.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    unused_port = sock.getsockname()[1]
    sock.close()
    postgres_failed = False
    try:
        psycopg2.connect(host=settings.db_host, port=unused_port, dbname=settings.db_name,
                         user=settings.db_user, password=settings.db_password, connect_timeout=1)
    except psycopg2.OperationalError:
        postgres_failed = True
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            postgres_recovered = cur.fetchone()[0] == 1
    pg_result = {"name": "postgres_unreachable_endpoint_then_recovery",
                 "status": "passed" if postgres_failed and postgres_recovered else "failed",
                 "unreachable_endpoint_rejected": postgres_failed, "live_database_query_afterward": postgres_recovered,
                 "scope_note": "Tests connection failure handling without interrupting the PostgreSQL service."}

    kafka_failed = False
    bad_producer = None
    try:
        bad_producer = KafkaProducer(bootstrap_servers=["127.0.0.1:1"], api_version=(2, 0, 0),
                                     api_version_auto_timeout_ms=1000, request_timeout_ms=1000,
                                     max_block_ms=1500, retries=0, acks=1)
        bad_producer.send("unavailable-broker-test", key=b"probe", value=b"probe").get(timeout=2)
    except KafkaError:
        kafka_failed = True
    except Exception:
        kafka_failed = True
    finally:
        if bad_producer is not None:
            bad_producer.close(timeout=2)
    topic, _group = new_topic("health-probe")
    healthy = KafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers, request_timeout_ms=5000)
    try:
        broker_recovered = bool(healthy.partitions_for(topic))
    finally:
        healthy.close(timeout=5)
    kafka_result = {"name": "unreachable_kafka_endpoint_then_live_broker_check",
                    "status": "passed" if kafka_failed and broker_recovered else "failed",
                    "unreachable_endpoint_rejected": kafka_failed, "live_broker_healthy_afterward": broker_recovered,
                    "scope_note": "Does not stop the shared Kafka broker; actual broker outage/restart is not tested."}
    return [pg_result, kafka_result]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    results = [test_consumer_restart(), test_duplicate_delivery(), test_injected_postgres_outage_replay(),
               test_api_timeout(), test_api_retry_recovers()]
    results.extend(test_unavailable_endpoints())
    output = {"created_at_utc": datetime.now(timezone.utc).isoformat(), "results": results,
              "not_tested": [
                  {"name": "actual_postgresql_service_outage", "reason": "Stopping the shared database would disrupt existing services; a transient persistence failure was injected instead."},
                  {"name": "actual_kafka_broker_outage_or_restart", "reason": "The shared broker was left running; an unreachable endpoint and subsequent live-broker check were used."},
              ]}
    path = PROJECT_ROOT / "docs" / "reliability_test_results.json"
    path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2))
    if any(r["status"] != "passed" for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
