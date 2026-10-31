"""DummyJSON client with pagination, validation, timeouts, and retries."""
import logging
from typing import Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .config import settings

logger = logging.getLogger(__name__)


class DummyJsonClient:
    def __init__(self, timeout: tuple[float, float] = (5, 30), retries: int = 4) -> None:
        self.timeout = timeout
        retry = Retry(total=retries, connect=retries, read=retries, status=retries, backoff_factor=0.6,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=frozenset({"GET"}), respect_retry_after_header=True)
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    def fetch_all(self, endpoint: str, page_size: int = 100) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        skip, total = 0, None
        while total is None or skip < total:
            response = self.session.get(f"{settings.api_base_url.rstrip('/')}/{endpoint.lstrip('/')}",
                                        params={"limit": page_size, "skip": skip}, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()
            batch = payload.get(endpoint)
            if not isinstance(batch, list):
                raise ValueError(f"DummyJSON /{endpoint} response is missing its list")
            try:
                page_total, page_skip = int(payload["total"]), int(payload["skip"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"DummyJSON /{endpoint} pagination metadata is invalid") from exc
            if page_skip != skip or (total is not None and total != page_total):
                raise ValueError(f"DummyJSON /{endpoint} pagination changed unexpectedly")
            total = page_total
            if not batch and skip < total:
                raise ValueError(f"DummyJSON /{endpoint} returned an empty page before total")
            if any(not isinstance(row, dict) or not isinstance(row.get("id"), int) for row in batch):
                raise ValueError(f"DummyJSON /{endpoint} contains a record without an integer id")
            records.extend(batch)
            logger.info("Fetched %d/%d %s", len(records), total, endpoint)
            skip += len(batch)
            if not batch:
                break
        if len(records) != total:
            raise ValueError(f"Expected {total} /{endpoint} records, received {len(records)}")
        return records

    def close(self) -> None:
        self.session.close()
