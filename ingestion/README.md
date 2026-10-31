# Ingestion and streaming

The Python modules use the repository root `.env`; secrets are not stored in source.

```powershell
docker compose up -d postgres kafka
& .\.venv\Scripts\python.exe -m pip install -r .\ingestion\requirements.txt
& .\.venv\Scripts\python.exe -m ingestion.src.main
```

The API loader fetches all pages from DummyJSON `/products`, `/users`, and
`/carts`; validates records; retries transient HTTP failures; and transactionally
upserts to `api_products`, `api_users`, and `api_carts`. Each table retains the
complete source response as JSONB. Runs and errors are recorded in
`api_ingestion_runs`; logs are written to `ingestion/logs/pipeline.log`.

Generate a finite stream and consume it in another terminal:

```powershell
& .\.venv\Scripts\python.exe -m ingestion.src.streaming produce --count 10 --rate 60
& .\.venv\Scripts\python.exe -m ingestion.src.streaming consume --max-events 10
```

The producer labels records as simulated. The consumer stores events in
`simulated_events`, uses event IDs for duplicate protection, and commits Kafka
offsets after database persistence. Omit `--count` or `--max-events` for a
long-running service; Ctrl+C requests graceful shutdown.
