"""Transactional upsert and ingestion-run logging shared by API loaders."""
import json
import logging
from typing import Any
from psycopg2.extras import execute_values
from ..database import get_connection

logger = logging.getLogger(__name__)


def begin_run(entity: str) -> int:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO api_ingestion_runs (entity, started_at, status) VALUES (%s, NOW(), 'running') RETURNING run_id", (entity,))
            return cursor.fetchone()[0]


def fail_run(run_id: int, error: Exception) -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            message = f"{type(error).__name__}: {error}"
            cursor.execute("UPDATE api_ingestion_runs SET completed_at=NOW(), status='failed', error_message=%s WHERE run_id=%s", (message[:2000], run_id))


def run_load(entity: str, table: str, columns: list[str], records: list[dict[str, Any]], run_id: int | None = None) -> int:
    run_id = begin_run(entity) if run_id is None else run_id
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                if records:
                    json_columns = {"raw_data", "products"}
                    values = [tuple(json.dumps(row.get(key)) if key in json_columns else row.get(key) for key in columns) for row in records]
                    update_columns = [col for col in columns if col not in {"id", "source_system"}]
                    sql = (f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT (id) DO UPDATE SET "
                           + ", ".join(f"{col}=EXCLUDED.{col}" for col in update_columns)
                           + ", source_system=EXCLUDED.source_system, ingested_at=NOW()")
                    execute_values(cursor, sql, values)
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE api_ingestion_runs SET completed_at=NOW(), fetched_count=%s, loaded_count=%s, status='success' WHERE run_id=%s", (len(records), len(records), run_id))
    except Exception as exc:
        fail_run(run_id, exc)
        raise
    logger.info("Upserted %d %s records (run_id=%d)", len(records), entity, run_id)
    return len(records)
