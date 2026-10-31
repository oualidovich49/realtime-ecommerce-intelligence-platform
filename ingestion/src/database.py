"""PostgreSQL connection and non-destructive pipeline schema setup."""
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import psycopg2
from psycopg2.extensions import connection
from .config import PROJECT_ROOT, settings


def connect() -> connection:
    return psycopg2.connect(host=settings.db_host, port=settings.db_port, dbname=settings.db_name,
                            user=settings.db_user, password=settings.db_password, connect_timeout=10,
                            application_name="ecommerce_intelligence_pipeline")


@contextmanager
def get_connection() -> Iterator[connection]:
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def apply_sql_file(path: Path) -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(path.read_text(encoding="utf-8"))


def ensure_pipeline_schema() -> None:
    apply_sql_file(PROJECT_ROOT / "database" / "api_staging.sql")
    apply_sql_file(PROJECT_ROOT / "database" / "api_views.sql")
