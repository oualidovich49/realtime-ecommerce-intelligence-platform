-- Read-only PostgreSQL inspection queries used for Phase 6.

-- Database and target table sizes.
SELECT current_database(), pg_database_size(current_database()) AS database_bytes;
SELECT 'simulated_events' AS table_name,
       pg_total_relation_size('simulated_events'::regclass) AS total_bytes,
       count(*) AS rows
FROM simulated_events;
SELECT schemaname, relname AS table_name, indexrelname AS index_name, idx_scan,
       idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname IN ('simulated_events', 'api_products')
ORDER BY relname, indexrelname;

-- Session-local insert probe used by postgres_benchmark.py; disappears when its
-- database connection closes. The runner fills this with execute_values batches.
CREATE TEMP TABLE benchmark_insert_probe (
  id BIGINT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL,
  payload TEXT NOT NULL
) ON COMMIT PRESERVE ROWS;
-- Client-side template passed to psycopg2.extras.execute_values:
-- INSERT INTO benchmark_insert_probe (id,created_at,payload) VALUES %s

-- EXPLAIN ANALYZE queries from the benchmark. All are SELECT-only.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT event_type, count(*)
FROM simulated_events
WHERE consumed_at >= now() - interval '24 hours'
GROUP BY event_type;

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT count(*)
FROM simulated_events
WHERE event_type = 'product_viewed'
  AND consumed_at >= now() - interval '7 days';

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT date_trunc('day', p.created_at), sum(p.amount)
FROM payments p
JOIN orders o USING (order_id)
WHERE lower(p.payment_status) IN ('success', 'succeeded', 'completed')
GROUP BY 1
ORDER BY 1;

-- Collected pipeline metrics: one row per event-time second.
SELECT date_trunc('second', consumed_at) AS second,
       count(*) AS persisted_events_per_second,
       round(avg(processing_latency_ms), 2) AS event_to_pg_insert_ms
FROM simulated_events
WHERE consumed_at >= now() - interval '15 minutes'
GROUP BY 1 ORDER BY 1;

-- Consumer batch processing and reliability counts.
SELECT topic, consumer_group,
       sum(received_count) AS messages_received,
       sum(inserted_count) AS events_inserted,
       sum(duplicate_count) AS duplicates_suppressed,
       sum(failed_count) AS failed_events,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY processing_duration_ms) AS batch_p50_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY processing_duration_ms) AS batch_p95_ms,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY processing_duration_ms) AS batch_p99_ms
FROM streaming_consumer_batches
GROUP BY topic, consumer_group
ORDER BY max(completed_at) DESC;

-- Failed API runs and data-quality failures currently collected.
SELECT run_id, entity, status, started_at, completed_at, error_message
FROM api_ingestion_runs WHERE status = 'failed' ORDER BY started_at DESC;
SELECT run_id, rule_name, source_table, failed_rows, status, details
FROM data_quality_results WHERE status = 'fail' ORDER BY recorded_at DESC;
