"""Generate importable Grafana dashboard JSON using the provisioned PostgreSQL source."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = {"type": "grafana-postgresql-datasource", "uid": "postgres-ds"}
DATASET_NOTES = {
    "exec-overview": "Internal synthetic transactional data from PostgreSQL. Revenue and orders are sample records, not actual business activity.",
    "customer-activity": "Internal synthetic user_events data. These records do not represent real customer behavior.",
    "payment-monitoring": "Internal synthetic payment records from PostgreSQL; no real payment activity is shown.",
    "inventory-shipping": "Internal synthetic inventory and shipment records from PostgreSQL.",
    "data-engineering": "System events are synthetic. DummyJSON is external sample data. Kafka events are simulated, persisted to PostgreSQL, and measured through consumer batch telemetry.",
    "data-quality": "Automated PostgreSQL checks cover internal synthetic transactions, external DummyJSON sample data, and simulated event ingestion. This dashboard reports check results, not a claim that source datasets are production customer data.",
}
DATASET_BANNERS = {
    "exec-overview": "**Data source:** internal synthetic transaction records. These figures are sample data, not actual business activity.",
    "customer-activity": "**Data source:** internal synthetic user events. No real customer behavior is represented.",
    "payment-monitoring": "**Data source:** internal synthetic payment records. No real payment activity is represented.",
    "inventory-shipping": "**Data source:** internal synthetic inventory and shipment records.",
    "data-engineering": "**Data sources:** internal synthetic system events; external DummyJSON sample API history; simulated Kafka events persisted to PostgreSQL. Throughput, processing latency, duplicates, and failures are measured from consumer telemetry.",
    "data-quality": "**Scope:** automated checks on internal synthetic transactions, external DummyJSON sample records, and simulated event data. Results are stored in PostgreSQL; stale API ingestion is a warning, while integrity violations fail the run.",
}
PANEL_NOTES = {
    "Product Clicks": "Counts product_click and product_clicked events in internal synthetic user_events. A zero means no matching event was recorded in the selected range.",
    "Delayed Shipments": "An explicit placeholder is returned when there are no delayed shipments in the selected range.",
    "Errors by Severity": "An explicit zero-valued 'No errors' category is returned when there are no error-level system events in the selected range.",
    "Simulated Kafka Events": "Simulated Kafka events persisted to PostgreSQL. They are not real user activity.",
    "Event-to-Postgres Latency (ms)": "Elapsed time from the event timestamp to its PostgreSQL persistence timestamp; includes transport and consumer wait time.",
    "Kafka Consumer Processing Duration (ms)": "Time spent validating and persisting each consumed Kafka batch, recorded by the consumer.",
    "Consumer Errors by Type": "Invalid messages and PostgreSQL persistence errors recorded by the Kafka consumer.",
    "DummyJSON API Ingestion History": "External DummyJSON sample API run history; these rows are ingestion metadata, not sales activity.",
    "Total Revenue": "Successful internal synthetic payments in the selected time range, shown in USD.",
    "Payment Failure Rate": "Failed internal synthetic payments divided by all payments in the selected range, as a percentage from 0 to 100.",
    "Cart Abandonment": "Internal synthetic sessions with add_to_cart but no purchase in the selected time range, as a percentage from 0 to 100.",
}


def panel(panel_id, title, kind, sql, x, y, w=12, h=8):
    unit = "short"
    if "Revenue" in title or title == "Payment Volume Over Time":
        unit = "currencyUSD"
    elif "Rate" in title or "Abandonment" in title:
        unit = "percent"
    elif "Latency" in title:
        unit = "ms" if "(ms)" in title else "s"
    legend = kind in {"timeseries", "piechart"}
    result = {
        "id": panel_id, "title": title, "type": kind, "datasource": SOURCE,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}, "unit": unit, "decimals": 2 if unit != "short" else 0}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": legend},
                    "tooltip": {"mode": "single", "sort": "none"}},
        "targets": [{"refId": "A", "format": "time_series" if kind == "timeseries" else "table",
                     "rawQuery": True, "rawSql": sql, "editorMode": "code"}],
    }
    if title in PANEL_NOTES:
        result["description"] = PANEL_NOTES[title]
    if kind == "stat":
        result["options"]["reduceOptions"] = {"calcs": ["lastNotNull"], "fields": "", "values": False}
    if kind == "timeseries":
        result["options"]["showPoints"] = "always"
    if kind == "piechart":
        result["options"]["pieType"] = "pie"
        result["options"]["reduceOptions"] = {"calcs": ["lastNotNull"], "fields": "", "values": False}
    if title in {"Failed Transactions", "Payment Failure Rate", "Critical Errors"}:
        result["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
        result["fieldConfig"]["defaults"]["thresholds"] = {
            "mode": "absolute", "steps": ([{"color": "green", "value": None}, {"color": "yellow", "value": 5}, {"color": "red", "value": 10}]
                                                if title == "Payment Failure Rate" else
                                                [{"color": "green", "value": None}, {"color": "red", "value": 1}]),
        }
    return result


def source_banner(uid):
    return {
        "id": 99, "title": "Data source", "type": "text", "gridPos": {"x": 0, "y": 0, "w": 24, "h": 3},
        "options": {"mode": "markdown", "content": DATASET_BANNERS[uid]},
    }


def dashboard(uid, title, definitions):
    panels = [source_banner(uid)]
    y, col = 3, 0
    for i, (name, kind, query) in enumerate(definitions, 1):
        width = 24 if kind == "table" else 12
        if width == 24 and col:
            y += 8
            col = 0
        x = col * 12
        panels.append(panel(i, name, kind, query, x, y, width, 8))
        if width == 24:
            y += 8
        else:
            col += 1
            if col == 2:
                col = 0
                y += 8
    return {"id": None, "uid": uid, "title": title, "description": DATASET_NOTES[uid], "tags": ["ecommerce", "analytics"],
            "timezone": "browser", "schemaVersion": 39, "version": 1, "refresh": "30s",
            "time": {"from": "now-30d", "to": "now"}, "panels": panels,
            "annotations": {"list": []}, "templating": {"list": []}, "editable": True,
            "graphTooltip": 0, "links": [], "liveNow": False}


DASHBOARDS = [
    dashboard("exec-overview", "Executive Overview", [
        ("Total Revenue", "stat", "SELECT COALESCE(SUM(amount),0) AS value FROM payments WHERE lower(payment_status) IN ('success','succeeded','completed') AND $__timeFilter(created_at)"),
        ("Total Orders", "stat", "SELECT COUNT(*) AS value FROM orders WHERE $__timeFilter(created_at)"),
        ("Successful Payments", "stat", "SELECT COUNT(*) AS value FROM payments WHERE lower(payment_status) IN ('success','succeeded','completed') AND $__timeFilter(created_at)"),
        ("Revenue by Country", "barchart", "SELECT o.country, SUM(p.amount) AS revenue FROM payments p JOIN orders o USING(order_id) WHERE lower(p.payment_status) IN ('success','succeeded','completed') AND $__timeFilter(p.created_at) GROUP BY o.country ORDER BY revenue DESC"),
        ("Revenue Over Time", "timeseries", "SELECT $__timeGroup(time,'1d') AS time, SUM(revenue)::double precision AS revenue FROM v_revenue_over_time WHERE $__timeFilter(time) GROUP BY 1 ORDER BY 1"),
        ("Orders Over Time", "timeseries", "SELECT $__timeGroup(created_at,'1d') AS time, COUNT(*)::double precision AS orders FROM orders WHERE $__timeFilter(created_at) GROUP BY 1 ORDER BY 1"),
    ]),
    dashboard("customer-activity", "Customer Activity", [
        ("Total User Events", "stat", "SELECT COUNT(*) AS value FROM user_events WHERE $__timeFilter(event_time)"),
        ("Active Sessions", "stat", "SELECT COUNT(DISTINCT session_id) AS value FROM user_events WHERE $__timeFilter(event_time)"),
        ("Page Views", "stat", "SELECT COUNT(*) AS value FROM user_events WHERE event_type='page_view' AND $__timeFilter(event_time)"),
        ("Product Clicks", "stat", "SELECT COUNT(*) AS value FROM user_events WHERE lower(event_type) IN ('product_click','product_clicked') AND $__timeFilter(event_time)"),
        ("Cart Abandonment", "stat", "WITH session_activity AS (SELECT session_id, bool_or(event_type='add_to_cart') AS added_to_cart, bool_or(event_type='purchase') AS purchased FROM user_events WHERE session_id IS NOT NULL AND $__timeFilter(event_time) GROUP BY session_id) SELECT round(100.0*count(*) FILTER (WHERE added_to_cart AND NOT purchased)::numeric/NULLIF(count(*) FILTER (WHERE added_to_cart),0)::numeric,2) AS value FROM session_activity"),
        ("Device Distribution", "piechart", "SELECT COUNT(*) FILTER (WHERE lower(device_type)='desktop')::double precision AS desktop, COUNT(*) FILTER (WHERE lower(device_type)='mobile')::double precision AS mobile, COUNT(*) FILTER (WHERE lower(device_type) NOT IN ('desktop','mobile'))::double precision AS other FROM user_events WHERE $__timeFilter(event_time)"),
        ("Activity by Country", "barchart", "SELECT country, COUNT(*) AS total_events FROM user_events WHERE $__timeFilter(event_time) GROUP BY country ORDER BY total_events DESC"),
    ]),
    dashboard("payment-monitoring", "Payment Monitoring", [
        ("Total Payments", "stat", "SELECT COUNT(*) AS value FROM payments WHERE $__timeFilter(created_at)"),
        ("Successful Transactions", "stat", "SELECT COUNT(*) AS value FROM payments WHERE lower(payment_status) IN ('success','succeeded','completed') AND $__timeFilter(created_at)"),
        ("Failed Transactions", "stat", "SELECT COUNT(*) AS value FROM payments WHERE lower(payment_status) IN ('failed','failure') AND $__timeFilter(created_at)"),
        ("Payment Failure Rate", "stat", "SELECT COALESCE(100.0*COUNT(*) FILTER (WHERE lower(payment_status) IN ('failed','failure'))/NULLIF(COUNT(*),0),0) AS value FROM payments WHERE $__timeFilter(created_at)"),
        ("Payment Method Distribution", "piechart", "SELECT COUNT(*) FILTER (WHERE lower(payment_method)='card')::double precision AS card, COUNT(*) FILTER (WHERE lower(payment_method)='paypal')::double precision AS paypal, COUNT(*) FILTER (WHERE lower(payment_method) NOT IN ('card','paypal'))::double precision AS other FROM payments WHERE $__timeFilter(created_at)"),
        ("Payment Volume Over Time", "timeseries", "SELECT $__timeGroup(created_at,'1d') AS time, SUM(amount)::double precision AS amount FROM payments WHERE $__timeFilter(created_at) GROUP BY 1 ORDER BY 1"),
        ("Recent Failed Payments", "table", "SELECT transaction_id, order_id, payment_method, amount, currency, payment_status, created_at FROM v_recent_failed_payments WHERE $__timeFilter(created_at) ORDER BY created_at DESC"),
    ]),
    dashboard("inventory-shipping", "Inventory and Shipping", [
        ("Total Products", "stat", "SELECT COUNT(*) AS value FROM products"),
        ("Low Stock Products", "stat", "SELECT low_stock_products AS value FROM v_low_stock_products"),
        ("Inventory by Product", "barchart", "SELECT product_name, available_quantity FROM v_inventory_by_product ORDER BY available_quantity"),
        ("Inventory Events Over Time", "timeseries", "SELECT $__timeGroup(event_time,'1d') AS time, COUNT(*)::double precision AS events FROM inventory_events WHERE $__timeFilter(event_time) GROUP BY 1 ORDER BY 1"),
        ("Shipment Status Distribution", "piechart", "SELECT COUNT(*) FILTER (WHERE lower(shipment_status)='delivered')::double precision AS delivered, COUNT(*) FILTER (WHERE lower(shipment_status)='in_transit')::double precision AS in_transit, COUNT(*) FILTER (WHERE lower(shipment_status)='preparing')::double precision AS preparing, COUNT(*) FILTER (WHERE lower(shipment_status) NOT IN ('delivered','in_transit','preparing'))::double precision AS other FROM shipments WHERE $__timeFilter(updated_at)"),
        ("Delayed Shipments", "table", "WITH delayed AS (SELECT shipment_id, tracking_number, carrier, shipment_status, shipped_at, destination_country FROM v_delayed_shipments WHERE $__timeFilter(shipped_at)) SELECT shipment_id, tracking_number, carrier, shipment_status, shipped_at, destination_country FROM delayed UNION ALL SELECT NULL::bigint, 'No delayed shipments'::text, NULL::text, 'none'::text, NULL::timestamptz, NULL::text WHERE NOT EXISTS (SELECT 1 FROM delayed) ORDER BY shipped_at"),
        ("Shipments by Country", "barchart", "SELECT destination_country, COUNT(*) AS shipments FROM shipments WHERE $__timeFilter(updated_at) GROUP BY destination_country ORDER BY shipments DESC"),
    ]),
    dashboard("data-engineering", "Data Engineering Monitoring", [
        ("Total System Events", "stat", "SELECT COUNT(*) AS value FROM system_events WHERE $__timeFilter(event_time)"),
        ("Critical Errors", "stat", "SELECT COUNT(*) AS value FROM system_events WHERE upper(severity) IN ('CRITICAL','FATAL') AND $__timeFilter(event_time)"),
        ("Errors by Severity", "barchart", "WITH errors AS (SELECT severity, COUNT(*) AS events FROM system_events WHERE upper(severity) IN ('ERROR','CRITICAL','FATAL') AND $__timeFilter(event_time) GROUP BY severity) SELECT severity, events FROM errors UNION ALL SELECT 'No errors', 0::bigint WHERE NOT EXISTS (SELECT 1 FROM system_events WHERE upper(severity) IN ('ERROR','CRITICAL','FATAL') AND $__timeFilter(event_time))"),
        ("Events by Service", "barchart", "SELECT service_name, COUNT(*) AS events FROM system_events WHERE $__timeFilter(event_time) GROUP BY service_name ORDER BY events DESC"),
        ("Simulated Events Per Minute", "timeseries", "SELECT $__timeGroup(consumed_at,'1m') AS time, COUNT(*)::double precision AS events FROM simulated_events WHERE $__timeFilter(consumed_at) GROUP BY 1 ORDER BY 1"),
        ("Kafka Events per Second", "timeseries", "SELECT date_trunc('second',consumed_at) AS time, COUNT(*)::double precision AS events_per_second FROM simulated_events WHERE $__timeFilter(consumed_at) GROUP BY 1 ORDER BY 1"),
        ("Event Counts by Type", "barchart", "SELECT event_type, COUNT(*) AS events FROM simulated_events WHERE $__timeFilter(consumed_at) GROUP BY event_type ORDER BY events DESC"),
        ("Event-to-Postgres Latency (ms)", "timeseries", "SELECT $__timeGroup(consumed_at,'1m') AS time, AVG(processing_latency_ms)::double precision AS milliseconds FROM simulated_events WHERE $__timeFilter(consumed_at) GROUP BY 1 ORDER BY 1"),
        ("Kafka Consumer Processing Duration (ms)", "timeseries", "SELECT $__timeGroup(completed_at,'1m') AS time, AVG(processing_duration_ms)::double precision AS milliseconds FROM streaming_consumer_batches WHERE $__timeFilter(completed_at) GROUP BY 1 ORDER BY 1"),
        ("Consumer Errors by Type", "barchart", "WITH errors AS (SELECT error_type, COUNT(*) AS failures FROM streaming_consumer_errors WHERE $__timeFilter(recorded_at) GROUP BY error_type) SELECT error_type, failures FROM errors UNION ALL SELECT 'No consumer errors', 0::bigint WHERE NOT EXISTS (SELECT 1 FROM streaming_consumer_errors WHERE $__timeFilter(recorded_at))"),
        ("Consumer Failed or Partial Batches", "stat", "SELECT COUNT(*) AS value FROM streaming_consumer_batches WHERE status IN ('partial','failed') AND $__timeFilter(completed_at)"),
        ("Duplicate Events Suppressed", "stat", "SELECT COALESCE(SUM(duplicate_count),0) AS value FROM streaming_consumer_batches WHERE $__timeFilter(completed_at)"),
        ("DummyJSON API Ingestion History", "table", "SELECT entity, status, fetched_count, loaded_count, started_at, completed_at, COALESCE(error_message,'—') AS error_message FROM api_ingestion_runs WHERE $__timeFilter(started_at) ORDER BY started_at DESC"),
        ("Pipeline Processing Latency (ms)", "timeseries", "SELECT $__timeGroup(completed_at,'1h') AS time, AVG(EXTRACT(EPOCH FROM completed_at-started_at)*1000)::double precision AS milliseconds FROM api_ingestion_runs WHERE status='success' AND completed_at IS NOT NULL AND $__timeFilter(completed_at) GROUP BY 1 ORDER BY 1"),
        ("Failed API Ingestion Runs", "stat", "SELECT COUNT(*) AS value FROM api_ingestion_runs WHERE status='failed' AND $__timeFilter(started_at)"),
        ("Pipeline Health", "stat", "SELECT CASE WHEN (SELECT status FROM api_ingestion_runs ORDER BY started_at DESC LIMIT 1)='failed' OR (SELECT status FROM streaming_consumer_batches ORDER BY completed_at DESC LIMIT 1) IN ('failed','partial') OR (SELECT status FROM data_quality_runs ORDER BY started_at DESC LIMIT 1)='failed' THEN 'Attention' ELSE 'Healthy' END AS value"),
    ]),
    dashboard("data-quality", "Data Quality", [
        ("Latest Quality Run Status", "stat", "SELECT status AS value FROM data_quality_runs ORDER BY started_at DESC LIMIT 1"),
        ("Failed Checks", "stat", "SELECT COALESCE((SELECT checks_failed FROM data_quality_runs ORDER BY started_at DESC LIMIT 1),0) AS value"),
        ("Warnings", "stat", "SELECT COALESCE((SELECT checks_warned FROM data_quality_runs ORDER BY started_at DESC LIMIT 1),0) AS value"),
        ("Quality Runs Over Time", "timeseries", "SELECT $__timeGroup(started_at,'1d') AS time, COUNT(*) FILTER (WHERE status='passed')::double precision AS passed, COUNT(*) FILTER (WHERE status='warning')::double precision AS warning, COUNT(*) FILTER (WHERE status='failed')::double precision AS failed FROM data_quality_runs WHERE $__timeFilter(started_at) GROUP BY 1 ORDER BY 1"),
        ("Latest Check Results", "table", "SELECT r.rule_name, r.source_table, r.severity, r.status, r.failed_rows, r.details, r.recorded_at FROM data_quality_results r JOIN data_quality_runs q USING(run_id) WHERE q.run_id=(SELECT run_id FROM data_quality_runs ORDER BY started_at DESC LIMIT 1) ORDER BY r.result_id"),
        ("Findings by Rule", "barchart", "SELECT rule_name, SUM(failed_rows) AS findings FROM data_quality_results WHERE $__timeFilter(recorded_at) GROUP BY rule_name ORDER BY findings DESC, rule_name"),
    ]),
]


def main():
    out = ROOT / "dashboards"
    out.mkdir(parents=True, exist_ok=True)
    for item in DASHBOARDS:
        (out / f"{item['uid']}.json").write_text(json.dumps(item, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {item['uid']}.json")


if __name__ == "__main__":
    main()
