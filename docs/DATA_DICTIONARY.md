# Data dictionary

This dictionary describes the main analytical, ingestion, streaming, and quality tables. The deployed database remains the source of truth for exact types and constraints. **Internal** records are synthetic; **DummyJSON** records are externally sourced samples; **simulated** records are generated locally for the Kafka demonstration.

## Existing internal synthetic data

| Table | Key | Important fields | Meaning |
|---|---|---|---|
| `customers` | `customer_id` | `customer_name`, `email`, `country`, `city`, `created_at` | Illustrative customer dimension. |
| `products` | `product_id` | `sku`, `product_name`, `category`, `price`, `currency`, `created_at` | Illustrative product catalog. Prices are not live prices. |
| `orders` | `order_id` | `order_number`, `customer_id`, `order_status`, `total_amount`, `currency`, `country`, `created_at` | Illustrative order facts. |
| `order_items` | `order_item_id` | `order_id`, `product_id`, `quantity`, `unit_price`, `subtotal` | Order line facts and product/order relationships. |
| `payments` | `payment_id` | `transaction_id`, `order_id`, `payment_method`, `payment_status`, `amount`, `currency`, `created_at` | Synthetic payment outcomes and amounts. |
| `inventory` | `inventory_id` | `product_id`, `available_quantity`, `warehouse`, `updated_at` | Synthetic stock snapshot. |
| `inventory_events` | `inventory_event_id` | `product_id`, `event_type`, `quantity_changed`, `warehouse`, `event_time` | Synthetic stock changes. |
| `shipments` | `shipment_id` | `order_id`, `tracking_number`, `shipment_status`, `shipped_at`, `delivered_at`, `updated_at` | Synthetic fulfillment activity. |
| `user_events` | `event_id` | `session_id`, `customer_id`, `event_type`, `event_time`, `device_type`, `country` | Synthetic customer activity used by the activity dashboard. |
| `system_events` | `system_event_id` | `service_name`, `event_type`, `severity`, `event_time`, `message` | Synthetic operational events. |

Existing analytical views include revenue, recent failed payments, low stock, and delayed shipments. Views do not turn the synthetic records into actual business data.

## DummyJSON sample tables

Each table retains a complete source record in `raw_data` JSONB, records `source_system='dummyjson'`, and sets `ingested_at` when loaded. API IDs are source IDs and are independent from internal IDs.

| Table | Primary key | Selected fields |
|---|---|---|
| `api_products` | `id` | `title`, `description`, `category`, `price`, `discount_percentage`, `rating`, `stock`, `brand`, `sku`, `thumbnail`, `raw_data`, `source_system`, `ingested_at` |
| `api_users` | `id` | `username`, `email`, `first_name`, `last_name`, `gender`, `age`, `birth_date`, `country`, `city`, `raw_data`, `source_system`, `ingested_at` |
| `api_carts` | `id` | `user_id`, `total`, `discounted_total`, `total_products`, `total_quantity`, `products` JSONB, `raw_data`, `source_system`, `ingested_at` |
| `api_ingestion_runs` | `run_id` | `entity`, `started_at`, `completed_at`, `fetched_count`, `loaded_count`, `status`, `error_message` |

The `v_api_*` views provide reporting-friendly projections over the sample tables. DummyJSON carts are source sample data, not platform orders.

## Simulated streaming tables

| Table | Key | Important fields | Meaning |
|---|---|---|---|
| `simulated_events` | `event_id` UUID | `event_type`, `event_timestamp`, `customer_id`, `product_id`, `payload` JSONB, `source_system`, `consumed_at`, `processing_latency_ms` | Unique generated events persisted by the Kafka consumer. `processing_latency_ms` is event age at PostgreSQL row insertion, measured with `clock_timestamp()`; it includes Kafka/consumer wait and excludes transaction commit time. |
| `streaming_consumer_batches` | `batch_id` | `consumer_group`, `topic`, `started_at`, `completed_at`, `received_count`, `valid_count`, `inserted_count`, `duplicate_count`, `failed_count`, `processing_duration_ms`, `status` | Per-poll consumer throughput, write outcome, and monotonic batch work duration after poll, measured through SQL/telemetry statements and before transaction commit. |
| `streaming_consumer_errors` | `error_id` | `consumer_group`, `topic`, `partition_id`, `offset_number`, `event_id`, `error_type`, `error_message`, `recorded_at` | Invalid-message and persistence failure history. Messages are clipped to a bounded length. |

Supported event types: `product_viewed`, `product_clicked`, `add_to_cart`, `order_created`, `payment_succeeded`, `payment_failed`, `inventory_updated`, and `shipment_updated`. Generated payloads contain a simulated marker. These events are for demonstrating ingestion and monitoring, not actual customer activity or a source of transactional truth.

## Data quality tables

| Table | Key | Important fields | Meaning |
|---|---|---|---|
| `data_quality_runs` | `run_id` | `started_at`, `completed_at`, `status`, `checks_total`, `checks_passed`, `checks_failed`, `checks_warned` | One summary row per quality command execution. |
| `data_quality_results` | `result_id` | `run_id`, `rule_name`, `source_table`, `severity`, `checked_rows`, `failed_rows`, `status`, `details`, `recorded_at` | One result per configured check per run. Integrity violations fail; stale API ingestion is a warning. |

## Units and aggregation

- Monetary amounts are stored in their table's currency column; current internal dashboard amounts are USD.
- Percentages are rates from 0 to 100.
- Event throughput panels group persisted events by minute.
- Event-to-PostgreSQL latency measures wall-clock age at persistence; consumer processing duration measures validation and PostgreSQL batch work.
- Counts in consumer batch history count Kafka messages, while `inserted_count` counts newly persisted UUIDs and `duplicate_count` counts conflict-suppressed replays.
