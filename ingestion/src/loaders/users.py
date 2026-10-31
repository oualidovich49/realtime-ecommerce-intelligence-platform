from typing import Any
from .common import run_load


def load_users(records: list[dict[str, Any]], run_id: int | None = None) -> int:
    rows = []
    for u in records:
        address = u.get("address") or {}
        rows.append({"id": u["id"], "username": u.get("username"), "email": u.get("email"),
                     "first_name": u.get("firstName"), "last_name": u.get("lastName"),
                     "gender": u.get("gender"), "age": u.get("age"), "birth_date": u.get("birthDate"),
                     "phone": u.get("phone"), "country": address.get("country"), "city": address.get("city"),
                     "state": address.get("state"), "raw_data": u, "source_system": "dummyjson"})
    return run_load("users", "api_users", ["id", "username", "email", "first_name", "last_name", "gender",
                    "age", "birth_date", "phone", "country", "city", "state", "raw_data", "source_system"], rows, run_id)
