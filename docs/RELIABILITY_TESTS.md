# Phase 6 — Reliability test report

## Scope and safety

Reliability checks ran on 2026-10-31 using isolated Kafka topics/groups, tagged simulated records, a local delayed HTTP test server, and an injected transient failure at the consumer persistence boundary. PostgreSQL, Kafka, and Grafana services were not stopped or reconfigured. Existing transactional, API, and event rows were not deleted or updated. Benchmark/reliability events were appended to `simulated_events`, and each new Kafka topic is retained.

## Executed scenarios

| Scenario | Result | Evidence |
|---|---|---|
| Consumer restart | **Passed** | Five messages were delivered; one consumer persisted two and exited; a second consumer using the same group persisted the remaining three. Five unique event rows were present afterward. |
| Duplicate event delivery | **Passed** | Two Kafka deliveries with the same UUID produced one PostgreSQL row; telemetry recorded 1 inserted, 1 duplicate suppressed, and 0 failures. |
| Transient PostgreSQL persistence failure and replay | **Passed (injected)** | The first batch write raised an injected `OperationalError`. No event row was visible before retry, the failed batch was recorded, Kafka offsets remained uncommitted, and a restarted consumer replayed and persisted the event once. PostgreSQL itself stayed online. |
| API request timeout | **Passed** | A local test server delayed its response beyond the client's 100 ms read timeout. The request failed and ingestion history recorded failed runs 16, 17, and 18 across repeated check invocations. The HTTP adapter surfaced a wrapped `ConnectionError` containing the read-timeout detail. No API source rows were loaded or changed. |
| API retry recovery | **Passed** | A local server delayed its first response beyond the 150 ms timeout, then returned a valid page. The configured retry made a second request and fetched one record successfully. |
| PostgreSQL endpoint unavailable, then recovery | **Passed (connection check)** | A connection attempt to an unused local port was rejected; a subsequent `SELECT 1` against the configured database succeeded. |
| Kafka endpoint unavailable, then broker health | **Passed (connection check)** | A producer targeting an unreachable local endpoint failed within its timeout; metadata lookup against the configured live broker then succeeded. The broker was not restarted. |

Raw evidence, including the topic/group names and run ID, is in [reliability_test_results.json](reliability_test_results.json).

## Not tested

- **Actual PostgreSQL service outage:** not tested. Stopping the shared PostgreSQL service would interrupt other platform work. The consumer write failure was injected at the call boundary and tested the no-commit/replay path without affecting the service.
- **Actual Kafka broker outage/restart:** not tested. The shared broker was left running. An unreachable endpoint and subsequent live-broker check exercised client error/recovery behavior without disrupting the broker.
- **Power loss or host crash during commit:** not tested.
- **Multi-broker failover:** not tested; the local broker is a single-node setup.

## Monitoring changes and current state

The Data Engineering dashboard now includes persisted Kafka events per second, event-to-PostgreSQL insert age, consumer batch processing duration, failed API ingestion runs, and a pipeline health indicator. Data Quality continues to show recorded quality-run failures and warnings. Metrics come from the existing event, batch, error, API-run, and quality-result tables; the dashboard does not synthesize uncollected counters.

After provisioning reload, the Grafana API listed all six project dashboards plus the pre-existing global dashboard. All 50 SQL-backed panels across that provisioned set executed against PostgreSQL and returned rows. The controlled API timeout checks intentionally left failed ingestion-run rows (run 18 is the latest); the failed-run KPI reflects those test failures and the combined pipeline indicator reports `Attention` until a later successful API run is recorded. This is the expected visible outcome of the injected failure. The retry-recovery case exercised the client directly and did not load or alter API source rows.

## Reproduce

```powershell
cd C:\realtime-intelligence-platform
& .\.venv\Scripts\python.exe -m ingestion.benchmarks.reliability_check
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The checks use UUID-tagged synthetic events and create new topic/group names on each invocation. They retain their test rows/topics to preserve evidence; repeated runs therefore add storage. They do not clean up or truncate existing data.
