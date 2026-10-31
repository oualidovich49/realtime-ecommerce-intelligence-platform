from typing import Any
from .common import run_load


def load_products(records: list[dict[str, Any]], run_id: int | None = None) -> int:
    rows = [{"id": p["id"], "title": p.get("title"), "description": p.get("description"),
             "category": p.get("category"), "price": p.get("price"),
             "discount_percentage": p.get("discountPercentage"), "rating": p.get("rating"),
             "stock": p.get("stock"), "brand": p.get("brand"), "sku": p.get("sku"),
             "thumbnail": p.get("thumbnail"), "raw_data": p, "source_system": "dummyjson"} for p in records]
    return run_load("products", "api_products", ["id", "title", "description", "category", "price",
                    "discount_percentage", "rating", "stock", "brand", "sku", "thumbnail", "raw_data", "source_system"], rows, run_id)
