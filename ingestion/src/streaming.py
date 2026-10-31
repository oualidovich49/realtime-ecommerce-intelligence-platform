"""Simulated Kafka event producer and PostgreSQL-persisting consumer."""
import argparse
import json
import logging
import random
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError
from kafka.serializer import Serializer

from .config import settings
from .database import ensure_pipeline_schema, get_connection
from .utils.logging_config import configure_logging

logger = logging.getLogger(__name__)
EVENT_TYPES = (
    "product_viewed", "product_clicked", "add_to_cart", "order_created",
    "payment_succeeded", "payment_failed", "inventory_updated", "shipment_updated",
)
stop_event = threading.Event()


class JsonSerializer(Serializer):
    def serialize(self, topic: str, value: dict[str, Any]) -> bytes:
        return json.dumps(value).encode("utf-8")


class EventKeySerializer(Serializer):
    def serialize(self, topic: str, value: str) -> bytes:
        return value.encode("utf-8")


def stop(_signum: int, _frame: Any) -> None:
    stop_event.set()


def ensure_topic(topic: str | None = None) -> None:
    ensure_pipeline_schema()
    topic = topic or settings.kafka_topic
    admin = KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers,
                             client_id="ecommerce-topic-admin", request_timeout_ms=10000)
    try:
        try:
            admin.create_topics(
                {topic: {"num_partitions": 3, "replication_factor": 1}},
                timeout_ms=15000, wait_for_metadata=True,
            )
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()


def internal_ids() -> tuple[list[int], list[int]]:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT customer_id FROM customers ORDER BY customer_id")
            customers = [row[0] for row in cursor.fetchall()]
            cursor.execute("SELECT product_id FROM products ORDER BY product_id")
            products = [row[0] for row in cursor.fetchall()]
    return customers, products


def make_event(customers: list[int], products: list[int]) -> dict[str, Any]:
    event_type = random.choice(EVENT_TYPES)
    customer_id = random.choice(customers) if customers else None
    product_id = random.choice(products) if products else None
    quantity = random.randint(1, 3)
    payload: dict[str, Any] = {"simulated": True, "source": "local_event_generator"}
    if product_id is not None:
        payload["product_id"] = product_id
    if event_type in {"product_viewed", "product_clicked"}:
        payload.update({"page": f"/products/{product_id}", "dwell_seconds": random.randint(2, 90)})
    elif event_type == "add_to_cart":
        payload["quantity"] = quantity
    elif event_type == "order_created":
        payload.update({"simulated_order_id": str(uuid.uuid4()), "quantity": quantity,
                        "amount": round(random.uniform(15, 500), 2), "currency": "USD"})
    elif event_type in {"payment_succeeded", "payment_failed"}:
        payload.update({"simulated_transaction_id": str(uuid.uuid4()),
                        "payment_status": "failed" if event_type == "payment_failed" else "succeeded",
                        "amount": round(random.uniform(15, 500), 2), "payment_method": random.choice(["card", "wallet", "bank_transfer"])})
    elif event_type == "inventory_updated":
        payload.update({"quantity_delta": random.randint(-8, 12), "warehouse": f"WH-{random.randint(1, 4)}"})
    elif event_type == "shipment_updated":
        payload.update({"shipment_status": random.choice(["processing", "shipped", "in_transit", "delivered"]),
                        "simulated_tracking_number": f"SIM-{uuid.uuid4().hex[:12].upper()}"})
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer_id,
        "product_id": product_id,
        "payload": payload,
        "source_system": "simulated",
    }


def produce(count: int | None, rate_per_minute: int) -> None:
    if rate_per_minute < 1:
        raise ValueError("rate_per_minute must be positive")
    ensure_topic()
    customers, products = internal_ids()
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all", retries=5, max_in_flight_requests_per_connection=1,
        value_serializer=JsonSerializer(),
        key_serializer=EventKeySerializer(),
    )
    delay = 60 / rate_per_minute
    sent = 0
    try:
        while not stop_event.is_set() and (count is None or sent < count):
            event = make_event(customers, products)
            producer.send(settings.kafka_topic, key=event["event_id"], value=event).get(timeout=30)
            sent += 1
            logger.info("Produced simulated event %s (%s)", event["event_id"], event["event_type"])
            if count is None or sent < count:
                stop_event.wait(delay)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
    logger.info("Produced %d simulated events", sent)


def validate_event(event: Any) -> dict[str, Any]:
    required = {"event_id", "event_type", "event_timestamp", "payload"}
    if not isinstance(event, dict) or not required.issubset(event):
        raise ValueError("Event is missing required fields")
    try:
        event_id = str(uuid.UUID(str(event["event_id"])))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("event_id must be a UUID") from exc
    if event["event_type"] not in EVENT_TYPES:
        raise ValueError("event_type is not supported")
    try:
        event_timestamp = datetime.fromisoformat(str(event["event_timestamp"]).replace("Z", "+00:00"))
    except (ValueError, TypeError) as exc:
        raise ValueError("event_timestamp must be a valid ISO timestamp") from exc
    if event_timestamp.tzinfo is None:
        raise ValueError("event_timestamp must include a timezone")
    if not isinstance(event["payload"], dict):
        raise ValueError("payload must be a JSON object")
    clean = dict(event)
    clean["event_id"] = event_id
    clean["event_timestamp"] = event_timestamp
    for field in ("customer_id", "product_id"):
        value = clean.get(field)
        if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
            raise ValueError(f"{field} must be an integer or null")
    return clean


def persist_batch(
    events: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    consumer_group: str,
    topic: str,
    received_count: int,
    started_at: datetime,
    started_clock: float,
) -> tuple[int, float]:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            if events:
                cursor.executemany(
                    "INSERT INTO simulated_events (event_id,event_type,event_timestamp,customer_id,product_id,payload,source_system,consumed_at,processing_latency_ms) "
                    "VALUES (%s,%s,%s,%s,%s,%s::jsonb,'simulated',clock_timestamp(),GREATEST(0,EXTRACT(EPOCH FROM (clock_timestamp()-%s))*1000)::numeric) "
                    "ON CONFLICT (event_id) DO NOTHING",
                    [(e["event_id"], e["event_type"], e["event_timestamp"], e.get("customer_id"), e.get("product_id"), json.dumps(e["payload"]), e["event_timestamp"]) for e in events],
                )
                inserted_count = cursor.rowcount
            else:
                inserted_count = 0

            if errors:
                cursor.executemany(
                    "INSERT INTO streaming_consumer_errors (consumer_group,topic,partition_id,offset_number,event_id,error_type,error_message) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    [(consumer_group, topic, e.get("partition_id"), e.get("offset_number"), e.get("event_id"), e["error_type"], e["error_message"][:2000]) for e in errors],
                )

            duration_ms = round((time.perf_counter() - started_clock) * 1000, 3)
            status = "success" if not errors else ("partial" if events else "failed")
            cursor.execute(
                "INSERT INTO streaming_consumer_batches (consumer_group,topic,started_at,completed_at,received_count,valid_count,inserted_count,duplicate_count,failed_count,processing_duration_ms,status) "
                "VALUES (%s,%s,%s,clock_timestamp(),%s,%s,%s,%s,%s,%s,%s)",
                (consumer_group, topic, started_at, received_count, len(events), inserted_count,
                 len(events) - inserted_count, len(errors), duration_ms, status),
            )
    return inserted_count, duration_ms


def record_persistence_failure(
    consumer_group: str, topic: str, received_count: int, started_at: datetime,
    started_clock: float, error: Exception,
) -> None:
    duration_ms = round((time.perf_counter() - started_clock) * 1000, 3)
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO streaming_consumer_errors (consumer_group,topic,error_type,error_message) VALUES (%s,%s,%s,%s)",
                (consumer_group, topic, type(error).__name__, str(error)[:2000]),
            )
            cursor.execute(
                "INSERT INTO streaming_consumer_batches (consumer_group,topic,started_at,completed_at,received_count,valid_count,inserted_count,duplicate_count,failed_count,processing_duration_ms,status) "
                "VALUES (%s,%s,%s,clock_timestamp(),%s,0,0,0,%s,%s,'failed')",
                (consumer_group, topic, started_at, received_count, received_count, duration_ms),
            )


def consume(max_events: int | None, group_id: str, topic: str | None = None,
           timing: dict[str, float] | None = None) -> None:
    topic = topic or settings.kafka_topic
    ensure_topic(topic)
    consumer = KafkaConsumer(
        topic, bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id, enable_auto_commit=False, auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    processed = 0
    try:
        while not stop_event.is_set() and (max_events is None or processed < max_events):
            remaining = None if max_events is None else max_events - processed
            polled = consumer.poll(timeout_ms=1000, max_records=min(100, remaining) if remaining is not None else 100)
            messages = [message for partition_messages in polled.values() for message in partition_messages]
            if not messages:
                continue
            started_at = datetime.now(timezone.utc)
            started_clock = time.perf_counter()
            if timing is not None and "first_batch_started" not in timing:
                timing["first_batch_started"] = started_clock
            batch = []
            errors = []
            for message in messages:
                try:
                    event = validate_event(json.loads(message.value.decode("utf-8")))
                    batch.append(event)
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                    logger.warning("Invalid Kafka event at partition=%d offset=%d: %s", message.partition, message.offset, exc)
                    errors.append({
                        "partition_id": message.partition, "offset_number": message.offset,
                        "event_id": None, "error_type": type(exc).__name__, "error_message": str(exc),
                    })
            try:
                stored, duration_ms = persist_batch(batch, errors, group_id, topic,
                                                    len(messages), started_at, started_clock)
            except Exception as exc:
                logger.exception("Failed to persist Kafka batch; offsets remain uncommitted")
                try:
                    record_persistence_failure(group_id, topic, len(messages), started_at, started_clock, exc)
                except Exception:
                    logger.exception("Could not record the Kafka persistence failure in PostgreSQL")
                raise
            consumer.commit()
            if timing is not None:
                timing["last_batch_committed"] = time.perf_counter()
            processed += len(messages)
            logger.info("Consumed %d events (%d inserted, %d errors, %.3f ms)", len(messages), stored, len(errors), duration_ms)
    finally:
        consumer.close()
        if timing is not None and "first_batch_started" in timing and "last_batch_committed" in timing:
            timing["elapsed_seconds"] = timing["last_batch_committed"] - timing["first_batch_started"]
    logger.info("Consumer stopped after %d messages", processed)


def main() -> None:
    configure_logging()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, stop)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    producer = sub.add_parser("produce", help="generate simulated e-commerce events")
    producer.add_argument("--count", type=int, help="number of events; omit to run until stopped")
    producer.add_argument("--rate", type=int, default=settings.event_rate_per_minute, help="events per minute")
    consumer = sub.add_parser("consume", help="persist Kafka events in PostgreSQL")
    consumer.add_argument("--max-events", type=int, help="stop after this many consumed messages")
    consumer.add_argument("--group-id", default="ecommerce-postgres-sink")
    consumer.add_argument("--topic", help="Kafka topic (default: configured pipeline topic)")
    args = parser.parse_args()
    try:
        if args.command == "produce":
            produce(args.count, args.rate)
        else:
            consume(args.max_events, args.group_id, args.topic)
    except Exception:
        logging.getLogger(__name__).exception("Kafka streaming process failed")
        raise


if __name__ == "__main__":
    main()
