import logging
from ..config import PROJECT_ROOT


def configure_logging() -> None:
    log_dir = PROJECT_ROOT / "ingestion" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")],
        force=True,
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)
