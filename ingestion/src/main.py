"""Run a complete DummyJSON staging ingestion."""

import argparse
import logging

from .api_client import DummyJsonClient
from .database import ensure_pipeline_schema
from .loaders import load_carts, load_products, load_users
from .loaders.common import begin_run, fail_run
from .utils.logging_config import configure_logging


def ingest_endpoint(client: DummyJsonClient, endpoint: str, loader) -> int:
    run_id = begin_run(endpoint)
    try:
        records = client.fetch_all(endpoint)
        return loader(records, run_id)
    except Exception as exc:
        fail_run(run_id, exc)
        raise


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", choices=("all", "products", "users", "carts"), default="all",
                        help="ingest one endpoint or all three (default: all)")
    args = parser.parse_args(argv)
    ensure_pipeline_schema()
    client = DummyJsonClient()
    try:
        loaders = {"products": load_products, "users": load_users, "carts": load_carts}
        selected = loaders.items() if args.endpoint == "all" else ((args.endpoint, loaders[args.endpoint]),)
        for endpoint, loader in selected:
            ingest_endpoint(client, endpoint, loader)
    except Exception:
        logger.exception("DummyJSON ingestion failed")
        raise
    finally:
        client.close()
    logger.info("DummyJSON ingestion completed")


if __name__ == "__main__":
    main()
