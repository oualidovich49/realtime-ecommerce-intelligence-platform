"""Environment based configuration for the ingestion services."""
from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def _required(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    db_host: str = _required("DB_HOST", "localhost")
    db_port: int = int(_required("DB_PORT", "5432"))
    db_name: str = _required("DB_NAME", "ecommerce")
    db_user: str = _required("DB_USER", "analytics")
    db_password: str = _required("DB_PASSWORD")
    api_base_url: str = _required("API_BASE_URL", "https://dummyjson.com")
    kafka_bootstrap_servers: str = _required("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_topic: str = _required("KAFKA_TOPIC", "ecommerce.events")
    event_rate_per_minute: int = int(_required("EVENT_RATE_PER_MINUTE", "30"))


settings = Settings()
