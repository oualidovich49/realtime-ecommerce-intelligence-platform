# Phase 6 - Performance benchmark

## Environment and method

Measured on 2026-10-31 against the existing local Kafka 3.9.1 and PostgreSQL 16 services, using `C:\realtime-intelligence-platform\.venv\Scripts\python.exe`. Each case used a fresh Kafka topic and consumer group, the configured three-partition topic layout, acks=`all`, the existing Python consumer, and a unique `benchmark_run_id` in each simulated event payload. The consumer persisted every benchmark event to `simulated_events`; benchmark records remain in PostgreSQL and are explicitly marked in their payload. No source rows were deleted or updated.

Producer elapsed time uses Python's monotonic `perf_counter` and includes acknowledgements and flush. Consumer throughput uses the monotonic span from first poll result through the last committed PostgreSQL batch. Consumer batch processing latency is measured monotonically after Kafka poll through validation and SQL/telemetry work, before transaction commit. Event-to-PostgreSQL latency uses PostgreSQL `clock_timestamp()` at row insertion minus the timezone-aware producer event timestamp. It includes Kafka queue and consumer wait age; it is not consumer processing latency and does not include final transaction commit time.

## Kafka results

All four final benchmark cases passed. Produced means broker-acknowledged messages; consumed means Kafka messages processed by the consumer. Processing percentiles use batch samples; event-age percentiles use event rows.

| Case | Produced / consumed / inserted | Producer events/s | Consumer events/s | Batch processing p50 / p95 / p99 (ms) | Event-to-PG insert p50 / p95 / p99 (ms) | Failures / duplicates |
|---|---:|---:|---:|---:|---:|---:|
| Burst 100 | 100 / 100 / 100 | 603.5 | 781.9 | 18.6 / 87.0 / 87.0 | 98.3 / 152.9 / 154.4 | 0 / 0 |
| Burst 1,000 | 1,000 / 1,000 / 1,000 | 1,341.5 | 714.2 | 110.6 / 154.6 / 154.6 | 639.2 / 1,135.6 / 1,183.7 | 0 / 0 |
| Burst 5,000 | 5,000 / 5,000 / 5,000 | 1,679.8 | 773.9 | 98.7 / 165.2 / 223.3 | 2,921.6 / 5,050.7 / 5,176.8 | 0 / 0 |
| Continuous, 5 minutes at 20/s | 6,000 / 6,000 / 6,000 | 20.0 | 20.0 | 20.3 / 45.2 / 53.9 | 22.1 / 56.7 / 76.0 | 0 / 0 |

The continuous case ran for 300.004 seconds and drained in a 299.888-second consumer span. Its end-to-end rate was 19.93 events/s. It had 4,806 batch-latency samples and 6,000 event-age samples. Burst batch percentiles had 2, 11, and 51 samples respectively, so small-case p95/p99 values are descriptive rather than stable tail estimates. Duplicate counts of zero are expected for these unique-ID cases; duplicate suppression was separately exercised by reliability tests.

Burst event age rises with event count while batch work remains around 0.10-0.22 seconds at 5,000. This indicates queue residence and burst draining dominate event age. During the continuous run, the consumer matched the 20 events/s input rate and event-to-insert p95 was 56.7 ms. The producer can enqueue faster than the single consumer drains, so a large burst creates several seconds of age even though it persists successfully.

Exact run IDs, topic names, means, sample counts, and per-case details are in [performance_benchmark_results.json](performance_benchmark_results.json).

An initial full pass was also run before changing the row timestamp from transaction-start `NOW()` to PostgreSQL `clock_timestamp()`. Those 12,100 tagged events were retained, but that pass is not the latency result summarized above. The saved results and table-growth baseline refer to the corrected final pass. The database currently contains 36,300 performance-tagged events from three complete passes, in addition to platform and reliability events.

## PostgreSQL measurements

The baseline was captured immediately before the final Kafka pass. The final snapshot was taken after 12,100 benchmark events had been persisted.

| Measurement | Observed |
|---|---:|
| 10,000 inserts into a session-local temporary table | 0.254 s, 39,427 rows/s |
| Database size | 31,955,991 -> 38,673,431 bytes (+6,717,440) |
| `simulated_events` table plus indexes | 11,862,016 -> 17,539,072 bytes (+5,677,056) |
| `simulated_events` rows | 24,235 -> 36,335 (+12,100) |
| `api_products` | 194 rows and 2,678,784 bytes before and after |

The temporary insert probe was dropped when its connection closed and did not modify persistent tables. Kafka benchmark events and their telemetry were retained. The size delta includes database storage overhead and row/index changes from the final benchmark window.

### Query timings

Wall-clock client timings are p50/p95/p99 across 30 executions. `EXPLAIN ANALYZE` is server execution time for one measured plan. The exact SQL is in [benchmark_queries.sql](benchmark_queries.sql); full plan nodes and buffers are in [postgres_benchmark_results.json](postgres_benchmark_results.json).

| Query | Wall time p50 / p95 / p99 (ms) | EXPLAIN ANALYZE (ms) | Observed plan |
|---|---:|---:|---|
| Recent simulated events grouped by type | 14.6 / 19.0 / 19.6 | 18.056 | Sequential scan of 36,335 rows; 1,720 shared buffers hit, none read from disk |
| Product-viewed events within seven days | 5.7 / 9.0 / 9.6 | 4.728 | Bitmap scan using `idx_simulated_events_type`, 4,505 matching rows |
| Daily synthetic revenue | 0.9 / 1.6 / 6.6 | 0.205 | Small hash join and sequential scans over the five-order sample |

PostgreSQL chose a sequential scan for the 24-hour aggregation because the range covered the small benchmark table. The selective event-type query used the existing event-type index. No indexes were added or removed. The SQL file also reports index scan counters for follow-up at larger data volumes.

## Bottlenecks and practical next steps

- **Burst backlog, rather than sustained rate, dominates latency.** At 5,000 events, event-to-insert p95 was 5.05 seconds while batch processing p95 was 165 ms. If larger bursts matter, first measure a controlled increase to consumer batch size or concurrency and compare database write load.
- **Consumer throughput was 714-782 events/s for bursts in these local runs.** The consumer uses `executemany` for individual event inserts. Comparing `execute_values` batch insertion while preserving conflict handling and commit-before-offset semantics is the most direct optimization candidate.
- **The time-only analytical query scans the full table at this size.** It took 18.1 ms server-side for 36,335 rows and read cached buffers. Recheck at representative production volume before adding a time index or partitioning; this local sample does not justify those changes.
- **The continuous target was 20 events/s.** It demonstrates steady-state behavior, not maximum capacity. Increase the paced rate in controlled increments before making capacity claims.
- **Topics and benchmark rows are retained.** Each invocation creates new topics and tagged PostgreSQL rows, so repeated runs consume storage. The runner deliberately does not delete benchmark data.

CPU, memory, disk write latency, WAL/checkpoint behavior, and broker saturation were not measured. The temporary-table rate is a client-side bulk-insert reference, not production-table throughput.

## Reproduce

From `C:\realtime-intelligence-platform`:

```powershell
& .\.venv\Scripts\python.exe -m ingestion.benchmarks.postgres_benchmark --capture-baseline
& .\.venv\Scripts\python.exe -m ingestion.benchmarks.kafka_benchmark --sizes 100 1000 5000 --continuous-seconds 300 --continuous-rate 20
& .\.venv\Scripts\python.exe -m ingestion.benchmarks.postgres_benchmark --measure
```

Each Kafka case creates an isolated topic/group and stores a JSON summary. The runner uses monotonic timing for throughput and waits until tagged PostgreSQL rows are visible before reporting success.
