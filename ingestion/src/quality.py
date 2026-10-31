"""Read-only source checks recorded as durable PostgreSQL run history."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from .database import ensure_pipeline_schema, get_connection
from .utils.logging_config import configure_logging


CHECKS = [
    ("Null critical fields", "internal tables", "error", """
      SELECT (SELECT count(*) FROM customers WHERE customer_name IS NULL OR email IS NULL) +
             (SELECT count(*) FROM products WHERE product_name IS NULL OR price IS NULL) +
             (SELECT count(*) FROM orders WHERE customer_id IS NULL OR order_number IS NULL OR total_amount IS NULL) +
             (SELECT count(*) FROM order_items WHERE order_id IS NULL OR product_id IS NULL OR quantity IS NULL) +
             (SELECT count(*) FROM payments WHERE order_id IS NULL OR payment_status IS NULL OR amount IS NULL) +
             (SELECT count(*) FROM simulated_events WHERE event_type IS NULL OR event_timestamp IS NULL OR payload IS NULL)
    """),
    ("Duplicate IDs", "internal and API tables", "error", """
      SELECT (SELECT count(*) FROM (SELECT customer_id FROM customers GROUP BY customer_id HAVING count(*)>1) x) +
             (SELECT count(*) FROM (SELECT product_id FROM products GROUP BY product_id HAVING count(*)>1) x) +
             (SELECT count(*) FROM (SELECT order_id FROM orders GROUP BY order_id HAVING count(*)>1) x) +
             (SELECT count(*) FROM (SELECT id FROM api_products GROUP BY id HAVING count(*)>1) x) +
             (SELECT count(*) FROM (SELECT id FROM api_users GROUP BY id HAVING count(*)>1) x) +
             (SELECT count(*) FROM (SELECT id FROM api_carts GROUP BY id HAVING count(*)>1) x) +
             (SELECT count(*) FROM (SELECT event_id FROM simulated_events GROUP BY event_id HAVING count(*)>1) x)
    """),
    ("Invalid prices", "products, order_items, api_products", "error", """
      SELECT (SELECT count(*) FROM products WHERE price < 0) +
             (SELECT count(*) FROM order_items WHERE unit_price < 0 OR subtotal < 0) +
             (SELECT count(*) FROM api_products WHERE price < 0)
    """),
    ("Negative quantities", "inventory, order_items, api_carts", "error", """
      SELECT (SELECT count(*) FROM inventory WHERE available_quantity < 0) +
             (SELECT count(*) FROM order_items WHERE quantity < 0) +
             (SELECT count(*) FROM api_carts WHERE total_quantity < 0)
    """),
    ("Invalid payment statuses", "payments", "error", """
      SELECT count(*) FROM payments WHERE lower(trim(payment_status)) NOT IN
      ('success','succeeded','completed','failed','failure','pending','refunded','cancelled')
    """),
    ("Invalid timestamps", "transactional, API, and simulated events", "error", """
      SELECT (SELECT count(*) FROM orders WHERE created_at IS NULL OR created_at > now() + interval '5 minutes') +
             (SELECT count(*) FROM payments WHERE created_at IS NULL OR created_at > now() + interval '5 minutes') +
             (SELECT count(*) FROM user_events WHERE event_time IS NULL OR event_time > now() + interval '5 minutes') +
             (SELECT count(*) FROM simulated_events WHERE event_timestamp IS NULL OR event_timestamp > now() + interval '5 minutes') +
             (SELECT count(*) FROM api_products WHERE ingested_at IS NULL OR ingested_at > now() + interval '5 minutes') +
             (SELECT count(*) FROM api_users WHERE ingested_at IS NULL OR ingested_at > now() + interval '5 minutes') +
             (SELECT count(*) FROM api_carts WHERE ingested_at IS NULL OR ingested_at > now() + interval '5 minutes')
    """),
    ("Missing relationships", "transactional and API tables", "error", """
      SELECT (SELECT count(*) FROM orders o LEFT JOIN customers c USING(customer_id) WHERE c.customer_id IS NULL) +
             (SELECT count(*) FROM order_items i LEFT JOIN orders o USING(order_id) WHERE o.order_id IS NULL) +
             (SELECT count(*) FROM order_items i LEFT JOIN products p USING(product_id) WHERE p.product_id IS NULL) +
             (SELECT count(*) FROM payments p LEFT JOIN orders o USING(order_id) WHERE o.order_id IS NULL) +
             (SELECT count(*) FROM shipments s LEFT JOIN orders o USING(order_id) WHERE o.order_id IS NULL) +
             (SELECT count(*) FROM inventory i LEFT JOIN products p USING(product_id) WHERE p.product_id IS NULL) +
             (SELECT count(*) FROM api_carts c LEFT JOIN api_users u ON u.id=c.user_id WHERE u.id IS NULL)
    """),
    ("Stale API ingestion", "api_products, api_users, api_carts", "warning", """
      SELECT CASE WHEN (SELECT count(*) FROM api_products) = 0 OR
                        (SELECT count(*) FROM api_users) = 0 OR
                        (SELECT count(*) FROM api_carts) = 0 OR
                        (SELECT max(ingested_at) FROM api_products) < now() - make_interval(hours => %s) OR
                        (SELECT max(ingested_at) FROM api_users) < now() - make_interval(hours => %s) OR
                        (SELECT max(ingested_at) FROM api_carts) < now() - make_interval(hours => %s)
                   THEN 1 ELSE 0 END
    """),
]
CHECK_SCOPE_TABLES = {
    "Null critical fields": ("customers", "products", "orders", "order_items", "payments", "simulated_events"),
    "Duplicate IDs": ("customers", "products", "orders", "api_products", "api_users", "api_carts", "simulated_events"),
    "Invalid prices": ("products", "order_items", "api_products"),
    "Negative quantities": ("inventory", "order_items", "api_carts"),
    "Invalid payment statuses": ("payments",),
    "Invalid timestamps": ("orders", "payments", "user_events", "simulated_events", "api_products", "api_users", "api_carts"),
    "Missing relationships": ("orders", "order_items", "payments", "shipments", "inventory", "api_carts"),
    "Stale API ingestion": ("api_products", "api_users", "api_carts"),
}


def run_checks(stale_after_hours: int | None = None) -> tuple[int, str]:
    stale_after_hours = stale_after_hours or int(os.getenv("API_STALE_AFTER_HOURS", "24"))
    if stale_after_hours < 1:
        raise ValueError("API_STALE_AFTER_HOURS must be at least one hour")
    ensure_pipeline_schema()
    started_at = datetime.now(timezone.utc)
    results = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO data_quality_runs (started_at,status,checks_total) VALUES (%s,'running',%s) RETURNING run_id", (started_at, len(CHECKS)))
            run_id = cur.fetchone()[0]
            for rule_name, source_table, severity, sql in CHECKS:
                if rule_name == "Stale API ingestion":
                    cur.execute(sql, (stale_after_hours,) * 3)
                else:
                    cur.execute(sql)
                failed_rows = int(cur.fetchone()[0] or 0)
                tables = CHECK_SCOPE_TABLES[rule_name]
                cur.execute("SELECT " + " + ".join(f"(SELECT count(*) FROM {table})" for table in tables))
                checked_rows = int(cur.fetchone()[0] or 0)
                status = "pass" if failed_rows == 0 else ("warn" if severity == "warning" else "fail")
                results.append((rule_name, source_table, severity, failed_rows, status))
                cur.execute(
                    "INSERT INTO data_quality_results (run_id,rule_name,source_table,severity,checked_rows,failed_rows,status,details) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (run_id, rule_name, source_table, severity, checked_rows, failed_rows, status,
                     "No offending rows found." if not failed_rows else f"{failed_rows} finding(s); inspect the named source tables."),
                )
            failed = sum(row[-1] == "fail" for row in results)
            warned = sum(row[-1] == "warn" for row in results)
            passed = len(results) - failed - warned
            overall = "failed" if failed else ("warning" if warned else "passed")
            cur.execute(
                "UPDATE data_quality_runs SET completed_at=NOW(),status=%s,checks_passed=%s,checks_failed=%s,checks_warned=%s WHERE run_id=%s",
                (overall, passed, failed, warned, run_id),
            )
    print(f"Data quality run {run_id}: {overall} ({passed} passed, {failed} failed, {warned} warnings)")
    for name, _source, _severity, count, status in results:
        print(f"{status.upper():4} {name}: {count} finding(s)")
    return run_id, overall


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stale-after-hours", type=int)
    args = parser.parse_args()
    _, status = run_checks(args.stale_after_hours)
    if status == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
