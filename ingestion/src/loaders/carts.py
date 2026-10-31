from typing import Any
from .common import run_load


def load_carts(records: list[dict[str, Any]], run_id: int | None = None) -> int:
    rows = [{"id": c["id"], "user_id": c.get("userId"), "total": c.get("total"),
             "discounted_total": c.get("discountedTotal"), "total_products": c.get("totalProducts"),
             "total_quantity": c.get("totalQuantity"), "products": c.get("products", []),
             "raw_data": c, "source_system": "dummyjson"} for c in records]
    return run_load("carts", "api_carts", ["id", "user_id", "total", "discounted_total", "total_products",
                    "total_quantity", "products", "raw_data", "source_system"], rows, run_id)
