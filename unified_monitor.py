#!/usr/bin/env python3
"""
Unified Order & Service Monitor v6.5 HEADLESS (PATCHED)
========================================================
Combines:
  1. Order Auto-Monitor v5.5 — Instant Payment Alert + Pre-Move Validation (Loki/Grafana)
  2. Multi-Service Status Monitor — ConnectX + Stripe status page monitoring
  3. Loki Error Pattern Monitor — Error spike detection
  4. API Latency Monitor — Top 5 slowest API calls (internal/external) with alerts

PATCH NOTES (v6.5):
  - FIX: Payment validation now uses mvno_order.payment_info instead of payment_orders table.
         Root cause: ConnectX/Amdocs payments were NOT written to payment_orders, causing
         false "No payment record" warnings on valid completed orders (e.g. Order #21280).
  - NEW: payment_info (JSON from Amdocs) is the single source of truth for payment confirmation.
         Logic: payment_info NOT NULL → ✅ CONFIRMED | payment_intent_id only → ⚠️ PENDING | neither → ❌
  - FIX: Eliminates 1 Databricks query per validation (was: 4 queries, now: 3 queries).
  - NEW: Amdocs Payment ID extracted from payment_info JSON and displayed in alerts.

PATCH NOTES (v6.4):
  - NEW: API Latency Monitor — polls Loki for step timing patterns (time: NNNNms),
         classifies each call as Internal or External, ranks top 5 by latency,
         and fires Slack alerts when any single call > 3s or total > 8s.
  - NEW: Top 5 slowest API calls included in ORDER PAYMENT ALERTs.
  - NEW: --latency-only flag to run only the latency monitor.
  - NEW: --no-latency flag to disable the latency monitor.
  - NEW: External API domain mapping (Boss API, ConnectX/Amdocs endpoints).

PATCH NOTES (v6.3):
  - FIX: Uptime % threshold lowered from 99% to 95% — uptime is HISTORICAL,
         not real-time. A brief outage earlier in the day would drop uptime to
         ~98.x% causing false "Degraded" alerts for recovered services.
  - FIX: Debounce increased from 2→3 consecutive checks (15 min at 5-min intervals)
         before confirming a service is down or recovered.
  - FIX: Alert cooldown of 30 minutes — same service cannot be re-alerted within
         30 min even if it briefly clears and re-triggers (prevents flapping alerts).
  - FIX: Recovery requires 3 consecutive healthy checks (was 2), preventing
         premature "recovered" → "down" → "recovered" alert cycling.

PATCH NOTES (v6.1):
  - FIX: Status monitor now alerts on first-run degraded services (was silently skipping)
  - FIX: Slack/Email senders now retry up to 3 times with backoff on failure
  - FIX: Alert delivery failures now logged at ERROR level (were silent)
  - NEW: --force-alert flag to re-trigger alerts for all currently impacted services/orders

Runs BOTH monitors in parallel threads from a single process.

ORDER MONITOR:
  - Polls Loki via Grafana for order lifecycle events
  - Fires Slack/Email alerts when order moves to inProgress (payment confirmed)
  - Performs Databricks pre-move validation (select_number_type, plan count, payment, refund)
  - Detects select_number_type changes for already-alerted orders

STATUS MONITOR:
  - Checks ConnectX status page (HTML scrape + API fallback)
  - Checks Stripe status page (JSON API)
  - Sends Slack alerts when services go down or recover

Usage:
  python3 unified_monitor.py                     # Run all monitors (foreground)
  python3 unified_monitor.py --order-only         # Only order monitor
  python3 unified_monitor.py --status-only        # Only status monitor
  python3 unified_monitor.py --latency-only       # Only API latency monitor
  python3 unified_monitor.py --no-latency         # All monitors except latency
  python3 unified_monitor.py --status-once         # Single status check and exit
  python3 unified_monitor.py --test-slack          # Test Slack integration
  python3 unified_monitor.py --force-alert         # Force re-trigger all current alerts
  nohup python3 unified_monitor.py &              # Background
  systemctl start unified-monitor                 # systemd

Environment variables:
  GRAFANA_USER            = LDAP username (required for order monitor)
  GRAFANA_PASSWORD        = LDAP password (required for order monitor)
  SLACK_WEBHOOK           = Slack webhook URL (optional, has default)
  SMTP_PASSWORD           = Gmail app password (optional, for email alerts)
  DATABRICKS_HOST         = Databricks workspace host
  DATABRICKS_TOKEN        = Databricks Personal Access Token
  DATABRICKS_WAREHOUSE    = Databricks SQL Warehouse ID

Dependencies:
  pip install requests beautifulsoup4
"""

import json, os, re, time, ssl, base64, signal, sys, threading, argparse, logging
import urllib.parse, urllib.request, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

try:
    import requests as req_lib
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                        SHARED CONFIGURATION                              ║
# ╚════════════════════════════════════════════════════════════════════════════╝

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK", "")
ORDER_SLACK_WEBHOOK_URL = os.environ.get("ORDER_SLACK_WEBHOOK", "")

# Global flag for --force-alert
FORCE_ALERT = False

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 1 — ORDER MONITOR CONFIG                      ║
# ╚════════════════════════════════════════════════════════════════════════════╝

ORDER_CONFIG = {
    "grafana_url": "https://grafana.rockstar-automations.com",
    "datasource_uid": "deytmfg3y53pca",
    "namespace": "mc-customer-journey-prod",
    "poll_interval_seconds": 60,
    "lookback_minutes": 30,
    "initial_lookback_minutes": 60,
}

EMAIL_CONFIG = {
    "enabled":        True,
    "to":             "shaikharief@gmail.com",
    "from":           "ordermonitor.alerts@gmail.com",
    "smtp_host":      "smtp.gmail.com",
    "smtp_port":      587,
    "smtp_user":      "ordermonitor.alerts@gmail.com",
    "smtp_password":  os.environ.get("SMTP_PASSWORD", ""),
    "use_tls":        True,
    "subject_prefix": "[ORDER MONITOR ALERT]",
    "cooldown_seconds": 300,
}

ORDER_SLACK_CONFIG = {
    "enabled": True,
    "webhook_url": ORDER_SLACK_WEBHOOK_URL or SLACK_WEBHOOK_URL,
    "cooldown_seconds": 300,
}

LOKI_ERROR_PATTERN_CONFIG = {
    "enabled": True,
    "namespace": "mc-customer-journey-prod",
    "app_label": "rockstar-cj",
    "poll_interval_seconds": 120,          # Check every 2 minutes
    "lookback_minutes": 5,                 # Look back 5 minutes each poll
    "error_patterns": (
        "error|exception|failed|failure|timeout|refused|denied|"
        "unauthorized|forbidden|invalid|null|undefined|crash|"
        "fatal|critical|warn|404|500|502|503|504"
    ),
    "alert_threshold": 10,                 # Alert if >= this many error lines in one poll
    "spike_multiplier": 3.0,               # Alert if errors > 3x the rolling average
    "rolling_window_size": 10,             # Rolling average over last N polls
    "cooldown_seconds": 600,               # Don't re-alert for same pattern within 10 min
    "max_log_lines_in_alert": 15,          # Max sample lines shown in Slack alert
}

LATENCY_MONITOR_CONFIG = {
    "enabled": True,
    "namespace": "mc-customer-journey-prod",
    "app_label": "rockstar-cj",
    "poll_interval_seconds": 120,           # Check every 2 minutes
    "lookback_minutes": 5,                  # Look back 5 minutes each poll
    "slow_threshold_ms": 3000,              # Alert if any single API call > 3s
    "total_threshold_ms": 8000,             # Alert if total request time > 8s
    "top_n": 5,                             # Show top N slowest APIs in alert
    "cooldown_seconds": 600,                # Don't re-alert within 10 min
    "max_sample_lines_in_alert": 10,        # Max sample lines in Slack alert
    # Known external API domains → friendly names
    "external_api_map": {
        "prod-boss-api.rockstar-automations.com": "Boss API",
        "billing.amdocs-dbs.com": "Amdocs Billing (ConnectX)",
        "account-management.amdocs-dbs.com": "Amdocs Account Mgmt (ConnectX)",
        "productcatalog.amdocs-dbs.com": "Amdocs Product Catalog (ConnectX)",
        "shoppingcart.amdocs-dbs.com": "Amdocs Shopping Cart (ConnectX)",
        "product-inventory-api.amdocs-dbs.com": "Amdocs Product Inventory (ConnectX)",
        "esim-adapter.amdocs-dbs.com": "Amdocs eSIM Adapter (ConnectX)",
        "party-management.amdocs-dbs.com": "Amdocs Party Mgmt (ConnectX)",
    },
    # Known internal service patterns
    "internal_patterns": [
        "UserProfile", "UserOnboarding", "UserAddress", "MyBatisPlus",
        "MvnoOrder", "Build cart items", "memory operation",
    ],
}


DATABRICKS_CONFIG = {
    "enabled": True,
    "host": os.environ.get("DATABRICKS_HOST", "dbc-b7af8d94-a7ba.cloud.databricks.com"),
    "token": os.environ.get("DATABRICKS_TOKEN", ""),
    "warehouse_id": os.environ.get("DATABRICKS_WAREHOUSE", "7352f0009fd4e049"),
    "catalog": "rds-prod_catalog",
    "schema": "cj_prod",
}

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 2 — STATUS MONITOR CONFIG                     ║
# ╚════════════════════════════════════════════════════════════════════════════╝

STATUS_CHECK_INTERVAL = 300  # 5 minutes
STATE_FILE = Path(__file__).parent / "monitor_state.json"
STATUS_LOG_FILE = Path(__file__).parent / "status_monitor.log"
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "")
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

SERVICES = {
    "connectx": {
        "name": "ConnectX",
        "icon": "\U0001f535",
        "status_url": "https://status.amdocs-connectx.com/en",
        "status_page_link": "https://status.amdocs-connectx.com/en",
        "type": "html_scrape",
        "api_endpoints": [
            "/api/v1/components",
            "/api/v2/components.json",
            "/api/components",
            "/summary.json",
            "/api/v2/summary.json",
        ],
        "enabled": True,
    },
}


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 1A — ORDER MONITOR HELPERS                    ║
# ╚════════════════════════════════════════════════════════════════════════════╝

def to_ist(u): return u + timedelta(hours=5, minutes=30)
def fmt_ist(u): return to_ist(u).strftime('%Y-%m-%d %H:%M:%S') + " IST"
def now_utc(): return datetime.utcnow()
def mins_since(u): return (now_utc() - u).total_seconds() / 60 if u else 0
def short_ist(u): return to_ist(u).strftime('%H:%M:%S')


def order_log(msg, level="INFO"):
    ts = fmt_ist(now_utc())
    print(f"[{ts}] [ORDER] [{level}] {msg}", flush=True)


def status_log(msg, level="INFO"):
    ts = fmt_ist(now_utc())
    print(f"[{ts}] [STATUS] [{level}] {msg}", flush=True)


def parse_user_agent(ua_str):
    """Parse user-agent string to (platform, device_model, app_version)."""
    if not ua_str:
        return None, None, None
    platform = device = version = None
    if "iPhone" in ua_str or "iPad" in ua_str or "iPod" in ua_str:
        platform = "iOS"
        m = re.search(r"(iPhone|iPad|iPod).*?OS\s+([\d_]+)", ua_str)
        if m:
            device = m.group(1) + f" (iOS {m.group(2).replace('_', '.')})"
        else:
            m2 = re.search(r"(iPhone|iPad|iPod)", ua_str)
            if m2: device = m2.group(1)
    elif "Android" in ua_str:
        platform = "Android"
        m = re.search(r"Android\s+([\d.]+);\s*(.+?)\)", ua_str)
        if m:
            model = m.group(2).strip()
            device = model if model and model != "K" else f"Android {m.group(1)}"
        else:
            m2 = re.search(r"Android\s+([\d.]+)", ua_str)
            if m2: device = f"Android {m2.group(1)}"
    elif "Macintosh" in ua_str or "Mac OS" in ua_str:
        platform = "Web (macOS)"
    elif "Windows" in ua_str:
        platform = "Web (Windows)"
    elif "Linux" in ua_str:
        platform = "Web (Linux)"
    else:
        platform = "Web"
    m = re.search(r"(?:rockstar|mvno|cj)[/-]v?([\d.]+)", ua_str, re.I)
    if m: version = m.group(1)
    if not version:
        m = re.search(r"Build/([A-Z0-9.]+)", ua_str)
        if m: version = m.group(1)
    return platform, device, version


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 1B — LOKI CLIENT                              ║
# ╚════════════════════════════════════════════════════════════════════════════╝

class LokiClient:
    def __init__(self):
        self.url = ORDER_CONFIG["grafana_url"].rstrip("/")
        self.ds = ORDER_CONFIG["datasource_uid"]
        self._working_headers = None

    def _all_auth_options(self):
        options = []
        api_key = os.environ.get("GRAFANA_API_KEY", "")
        if api_key:
            options.append(("API Key", {"Authorization": f"Bearer {api_key}"}))
        u = os.environ.get("GRAFANA_USER", "")
        p = os.environ.get("GRAFANA_PASSWORD", "")
        if u and p:
            creds = base64.b64encode(f"{u}:{p}".encode()).decode()
            options.append(("LDAP", {"Authorization": f"Basic {creds}"}))
        sess = os.environ.get("GRAFANA_SESSION", "")
        if sess:
            options.append(("Session Cookie", {
                "Cookie": f"grafana_session={sess}", "X-Grafana-Org-Id": "1",
            }))
        options.append(("MCP Basic Auth", {
            "Authorization": "Basic Z3JhZmFuYS1tY3AtYXV0aDotUj1+Nko2PXp5XHclVThdP3YsXA=="
        }))
        return options

    def _make_request(self, url, headers, timeout=30):
        ctx = ssl.create_default_context()
        req = urllib.request.Request(url, headers=headers)
        return urllib.request.urlopen(req, timeout=timeout, context=ctx)

    def test_connection(self):
        test_url = f"{self.url}/api/datasources/uid/{self.ds}"
        for name, auth_headers in self._all_auth_options():
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            headers.update(auth_headers)
            try:
                with self._make_request(test_url, headers, timeout=10) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    self._working_headers = headers
                    return True, f"Connected via {name}! Datasource: {data.get('name','?')}", name
            except Exception:
                continue
        return False, "All auth methods failed. Set GRAFANA_USER+GRAFANA_PASSWORD.", ""

    def query(self, logql, start, end, limit=1000):
        params = urllib.parse.urlencode({
            "query": logql,
            "start": str(int(start.timestamp() * 1e9)),
            "end": str(int(end.timestamp() * 1e9)),
            "limit": str(limit), "direction": "forward",
        })
        url = f"{self.url}/api/datasources/proxy/uid/{self.ds}/loki/api/v1/query_range?{params}"
        if not self._working_headers:
            self._working_headers = {
                "Content-Type": "application/json", "Accept": "application/json",
                "Authorization": "Basic Z3JhZmFuYS1tY3AtYXV0aDotUj1+Nko2PXp5XHclVThdP3YsXA==",
            }
        try:
            with self._make_request(url, self._working_headers, timeout=60) as resp:
                result = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = ""
            try: body = e.read().decode("utf-8")[:300]
            except: pass
            raise Exception(f"HTTP {e.code} {e.reason} -- {body}")
        except urllib.error.URLError as e:
            raise Exception(f"Connection failed: {e.reason}")
        logs = []
        for s in result.get("data", {}).get("result", []):
            lb = s.get("stream", {})
            for ts_ns, line in s.get("values", []):
                logs.append((int(ts_ns), lb, line))
        return logs


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 1C — DATABRICKS SQL CLIENT                    ║
# ╚════════════════════════════════════════════════════════════════════════════╝

class DatabricksClient:
    """Queries Databricks SQL via the Statement Execution API."""

    def __init__(self):
        self.host = DATABRICKS_CONFIG.get("host", "").rstrip("/")
        self.token = DATABRICKS_CONFIG.get("token", "")
        self.warehouse_id = DATABRICKS_CONFIG.get("warehouse_id", "")
        self.catalog = DATABRICKS_CONFIG.get("catalog", "rds-prod_catalog")
        self.schema = DATABRICKS_CONFIG.get("schema", "cj_prod")
        self._enabled = bool(self.host and self.token and self.warehouse_id)

    @property
    def is_enabled(self):
        return self._enabled and DATABRICKS_CONFIG.get("enabled", False)

    def execute_sql(self, statement, timeout=120):
        """Execute a SQL statement and return list of row dicts."""
        if not self.is_enabled:
            return None, "Databricks not configured (set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE)"
        url = f"https://{self.host}/api/2.0/sql/statements"
        payload = json.dumps({
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "wait_timeout": "50s",
            "catalog": self.catalog,
            "schema": self.schema,
        }).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                result = json.loads(resp.read().decode("utf-8"))
            status = result.get("status", {}).get("state", "")
            if status != "SUCCEEDED":
                return None, f"Query status: {status}"
            manifest = result.get("manifest", {})
            columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
            data_array = result.get("result", {}).get("data_array", [])
            rows = []
            for row_obj in data_array:
                row = {}
                if isinstance(row_obj, list):
                    for i, col_name in enumerate(columns):
                        if i < len(row_obj):
                            val = row_obj[i]
                            row[col_name] = val if val is not None else None
                        else:
                            row[col_name] = None
                elif isinstance(row_obj, dict):
                    values = row_obj.get("values", [])
                    for i, col_name in enumerate(columns):
                        if i < len(values):
                            val_obj = values[i]
                            if isinstance(val_obj, dict):
                                if "string_value" in val_obj:
                                    row[col_name] = val_obj["string_value"]
                                elif "null_value" in val_obj:
                                    row[col_name] = None
                                else:
                                    row[col_name] = str(val_obj)
                            else:
                                row[col_name] = val_obj if val_obj is not None else None
                        else:
                            row[col_name] = None
                else:
                    continue
                rows.append(row)
            return rows, None
        except urllib.error.HTTPError as e:
            body = ""
            try: body = e.read().decode("utf-8")[:300]
            except: pass
            return None, f"Databricks HTTP {e.code}: {body}"
        except Exception as e:
            return None, f"Databricks error: {e}"

    def get_order_validation(self, order_id):
        """
        Pre-move validation for an order:
        1. select_number_type + payment_info from mvno_order
        2. Plan count (single plan check)
        3. Payment validation via mvno_order.payment_info (NOT payment_orders)
        4. Refund status from refund_records
        """
        result = {
            "select_number_type": None, "portin_status": None, "mvno_status": None,
            "plan_count": 0, "product_name": None, "payment_status": None,
            "payment_validated": False, "refund_status": None, "refund_amount": None,
            "refund_currency": None, "refund_reason": None, "refund_type": None,
            "has_refund": False, "can_move": False, "warnings": [], "errors": [],
            "db_error": None,
        }

        # Query 1: mvno_order (includes payment validation fields)
        sql_mvno = (
            f"SELECT order_id, select_number_type, portin_status, status, product_name, user_id, "
            f"payment_info, payment_intent_id, amount, currency "
            f"FROM `{self.catalog}`.`{self.schema}`.mvno_order "
            f"WHERE order_id = '{order_id}'"
        )
        rows, err = self.execute_sql(sql_mvno)
        if err:
            result["db_error"] = err
            result["errors"].append(f"DB query failed: {err}")
            return result
        if not rows:
            result["errors"].append(f"Order {order_id} NOT FOUND in mvno_order table")
            return result

        row = rows[0]
        result["select_number_type"] = row.get("select_number_type", "UNKNOWN")
        result["portin_status"] = row.get("portin_status")
        result["mvno_status"] = row.get("status")
        result["product_name"] = row.get("product_name")
        user_id = row.get("user_id")
        payment_info = row.get("payment_info")
        payment_intent_id = row.get("payment_intent_id")
        mvno_amount = row.get("amount")
        mvno_currency = row.get("currency", "USD")

        snt = result["select_number_type"]
        if snt == "NONE" or snt is None:
            result["errors"].append(
                "select_number_type = NONE -> Customer has NOT selected any option. DO NOT MOVE this order!")
        elif snt in ("PORT_IN", "PORTIN"):
            pass
        elif snt == "NEW_NUMBER":
            pass
        else:
            result["warnings"].append(f"Unexpected select_number_type: {snt}")

        # Query 2: Plan count
        sql_plans = (
            f"SELECT COUNT(*) as plan_count "
            f"FROM `{self.catalog}`.`{self.schema}`.mvno_order "
            f"WHERE order_id = '{order_id}'"
        )
        rows2, err2 = self.execute_sql(sql_plans)
        if not err2 and rows2:
            cnt = int(rows2[0].get("plan_count", 0))
            result["plan_count"] = cnt
            if cnt > 1:
                result["errors"].append(
                    f"Multiple plans ({cnt}) found for this order. Customer must have only a SINGLE plan.")

        # Query 3: Payment validation (via mvno_order.payment_info — single source of truth)
        # payment_info IS NOT NULL + contains Amdocs Payment ID = payment confirmed
        # payment_intent_id IS NOT NULL but payment_info IS NULL = payment attempted but not confirmed
        if payment_info and payment_info.strip() not in ("", "null", "None", "[]"):
            result["payment_validated"] = True
            # Extract Amdocs payment ID from JSON for display
            try:
                import json as _json
                pi_data = _json.loads(payment_info)
                if isinstance(pi_data, list) and pi_data:
                    amdocs_pay_id = pi_data[0].get("id", "")
                    result["payment_status"] = f"CONFIRMED (Amdocs: {amdocs_pay_id[:24]}...)"
                else:
                    result["payment_status"] = "CONFIRMED (payment_info present)"
            except Exception:
                result["payment_status"] = "CONFIRMED (payment_info present)"
            if payment_intent_id:
                result["payment_status"] += f" | PI: {payment_intent_id[:25]}..."
        elif payment_intent_id and payment_intent_id.strip():
            # Payment attempted via Stripe but not yet confirmed by Amdocs
            result["payment_validated"] = False
            result["payment_status"] = f"PENDING (intent: {payment_intent_id[:25]}...)"
            result["warnings"].append(
                f"Payment intent exists ({payment_intent_id[:30]}...) but payment_info is NULL. "
                "Payment attempted but NOT confirmed by ConnectX/Amdocs yet.")
        else:
            result["payment_validated"] = False
            result["payment_status"] = "N/A"
            result["errors"].append(
                "Payment NOT validated. No payment_info or payment_intent_id found in mvno_order.")

        # Query 4: Refund status
        if user_id:
            sql_refund = (
                f"SELECT r.refund_status, r.refund_amount, r.currency, r.refund_reason, "
                f"r.refund_type, r.stripe_refund_id, r.created_at "
                f"FROM `{self.catalog}`.`{self.schema}`.refund_records r "
                f"WHERE r.user_id = '{user_id}' ORDER BY r.created_at DESC LIMIT 1"
            )
            rows4, err4 = self.execute_sql(sql_refund)
            if err4:
                result["warnings"].append(f"Refund check failed: {err4}")
            elif rows4:
                ref_row = rows4[0]
                result["has_refund"] = True
                result["refund_status"] = ref_row.get("refund_status")
                result["refund_amount"] = ref_row.get("refund_amount")
                result["refund_currency"] = ref_row.get("currency", "USD")
                result["refund_reason"] = ref_row.get("refund_reason")
                result["refund_type"] = ref_row.get("refund_type")
                rs = result["refund_status"]
                if rs and rs.upper() in ("PENDING", "PROCESSING"):
                    result["errors"].append(
                        f"Refund {rs.upper()} \u2014 ${result['refund_amount']} {result['refund_currency']}. "
                        "DO NOT MOVE this order while refund is in progress!")
                elif rs and rs.upper() in ("COMPLETED", "SUCCEEDED", "REFUNDED"):
                    result["errors"].append(
                        f"Refund COMPLETED \u2014 ${result['refund_amount']} {result['refund_currency']}. "
                        "This order has been refunded. DO NOT MOVE!")

        result["can_move"] = (
            len(result["errors"]) == 0 and
            result["select_number_type"] in ("NEW_NUMBER", "PORT_IN", "PORTIN") and
            result["plan_count"] == 1 and
            result["payment_validated"] and
            not result["has_refund"]
        )
        return result

    def format_validation_summary(self, order_id, v):
        """Format the validation result as a readable string for alerts."""
        lines = []
        lines.append("=" * 50)
        lines.append("PRE-MOVE VALIDATION (Databricks mvno_order)")
        lines.append("=" * 50)

        snt = v.get("select_number_type", "UNKNOWN")
        if snt == "NONE" or snt is None:
            snt_icon = "\u274c"
        elif snt in ("NEW_NUMBER", "PORT_IN", "PORTIN"):
            snt_icon = "\u2705"
        else:
            snt_icon = "\u26a0\ufe0f"
        lines.append(f"  Number Type:    {snt_icon} {snt}")

        if v.get("portin_status") and v["portin_status"] != "NONE":
            lines.append(f"  Port-In Status: {v['portin_status']}")

        pc = v.get("plan_count", 0)
        pc_icon = "\u2705" if pc == 1 else "\u274c"
        lines.append(f"  Single Plan:    {pc_icon} {pc} plan(s)")

        pv = v.get("payment_validated", False)
        pv_icon = "\u2705" if pv else "\u274c"
        ps = v.get("payment_status", "N/A")
        lines.append(f"  Payment:        {pv_icon} {ps}")

        if v.get("has_refund"):
            rs = v.get("refund_status", "UNKNOWN")
            ra = v.get("refund_amount", "?")
            rc = v.get("refund_currency", "USD")
            rt = v.get("refund_type", "")
            if rs and rs.upper() in ("PENDING", "PROCESSING"):
                ref_icon = "\u26a0\ufe0f"
            elif rs and rs.upper() in ("COMPLETED", "SUCCEEDED", "REFUNDED"):
                ref_icon = "\u274c"
            else:
                ref_icon = "\u26a0\ufe0f"
            lines.append(f"  Refund:         {ref_icon} {rs} \u2014 ${ra} {rc} ({rt})")
            if v.get("refund_reason"):
                reason = v["refund_reason"][:80]
                lines.append(f"  Refund Reason:  {reason}")
        else:
            lines.append(f"  Refund:         \u2705 No refund")

        if v.get("product_name"):
            lines.append(f"  Product:        {v['product_name']}")

        lines.append("-" * 50)
        if v.get("can_move"):
            lines.append("\u2705 VERDICT: OK TO MOVE (mark MSISDN as Done)")
        else:
            lines.append("\u274c VERDICT: DO NOT MOVE \u2014 issues found:")
            for e in v.get("errors", []):
                lines.append(f"    \u274c {e}")
        for w in v.get("warnings", []):
            lines.append(f"    \u26a0\ufe0f {w}")
        lines.append("=" * 50)
        return "\n".join(lines)


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 1D — ORDER RECORD                             ║
# ╚════════════════════════════════════════════════════════════════════════════╝

class OrderRecord:
    def __init__(self, order_id):
        self.order_id = order_id
        self.first_seen = now_utc()
        self.last_activity = None
        self.state = None
        self.state_history = []
        self.became_draft_at = None
        self.became_acknowledged_at = None
        self.became_inprogress_at = None
        self.has_payment_info = False
        self.payment_page_loaded_at = None
        self.payment_intent_saved_at = None
        self.payment_intent_id = None
        self.connectx_at = None
        self.milestone_at = None
        self.esim_at = None
        self.amount = None
        self.currency = None
        self.product_name = None
        self.individual_id = None
        self.billing_account_id = None
        self.refund_initiated_at = None
        self.refund_completed_at = None
        self.refund_amount = None
        self.refund_currency = None
        self.refund_payment_id = None
        self.refund_customer_name = None
        self.customer_name = None
        self.customer_email = None
        self.sign_in_provider = None
        self.user_agent = None
        self.platform = None
        self.device_model = None
        self.app_version = None
        self.alert_fired = False
        self.resolved = False
        self.events = []
        self.error_count = 0
        self.last_error = None
        self.last_select_number_type = None
        self.select_number_type_history = []
        self.number_type_change_alert_fired = False
        self.draft_payment_alert_fired = False

    def set_state(self, ts, new_state, has_payment=False):
        old = self.state
        self.state = new_state.lower()
        self.state_history.append((ts, self.state))
        self.last_activity = ts
        if has_payment:
            self.has_payment_info = True
        if self.state == "draft" and not self.became_draft_at:
            self.became_draft_at = ts
        elif self.state == "acknowledged" and not self.became_acknowledged_at:
            self.became_acknowledged_at = ts
        elif self.state == "inprogress" and not self.became_inprogress_at:
            self.became_inprogress_at = ts

    @property
    def stage(self):
        if self.refund_completed_at: return "REFUNDED"
        if self.refund_initiated_at: return "REFUND PENDING"
        if self.resolved or self.state == "completed": return "COMPLETED"
        if self.esim_at: return "eSIM"
        if self.milestone_at: return "MILESTONE"
        if self.connectx_at and self.became_inprogress_at: return "PROVISIONING"
        if self.became_inprogress_at: return "IN PROGRESS"
        if self.became_acknowledged_at: return "ACKNOWLEDGED"
        if self.payment_page_loaded_at and self.state == "draft": return "PAYMENT PAGE"
        if self.state == "draft": return "DRAFT"
        return "SEEN"

    @property
    def is_in_progress(self):
        return (self.became_inprogress_at is not None and
                self.milestone_at is None and
                self.esim_at is None and
                self.refund_initiated_at is None and
                self.state not in ("completed",) and
                not self.resolved)

    def should_alert(self):
        if self.alert_fired and not FORCE_ALERT:
            return False
        if not self.became_inprogress_at:
            return False
        return True

    @property
    def duration_str(self):
        ref = self.became_inprogress_at or self.became_draft_at or self.first_seen
        m = int(mins_since(ref))
        if m < 1: return "<1m"
        if m < 60: return f"{m}m"
        return f"{m // 60}h {m % 60}m"

    @property
    def payment_str(self):
        if self.refund_completed_at:
            return f"Refunded ${self.refund_amount}" if self.refund_amount else "Refunded"
        if self.refund_initiated_at:
            return "Refund pending"
        if self.became_inprogress_at and self.has_payment_info:
            return f"Confirmed ${self.amount}" if self.amount else "Confirmed"
        if self.payment_page_loaded_at:
            return "Page loaded"
        return "\u2014"


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 1E — AUTO MONITOR ENGINE                      ║
# ╚════════════════════════════════════════════════════════════════════════════╝

class AutoMonitorEngine:
    PATTERNS = [
        (r"(?:Order status change event sent|Processing.*order event).*?orderId[=](\d{4,6}).*?state[=](\w+)", "STATE_CHANGE"),
        (r"Processing (\w+) status order.*?orderId[=](\d{4,6})", "STATE_PROCESSING"),
        (r"Starting to create Stripe Payment Intent.*?orderId[=: ]+(\d{4,6})", "PAYMENT_PAGE"),
        (r"(?:Successfully updated payment_intent_id|updated MVNO order payment intent).*?orderId[=: ]+(\d{4,6})", "INTENT_SAVED"),
        (r"Order info.*?orderId[=](\d{4,6})", "ORDER_INFO"),
        (r"crawl url.*?productOrder/(\d{4,6})", "CONNECTX"),
        (r"(?i)milestone.*?(?:updated|info).*?orderId[=: ]+(\d{4,6})", "MILESTONE"),
        (r"Order (?:already exists|created).*?orderId[=: ]+(\d{4,6})", "ORDER_EXISTS"),
        (r"Successfully saved MVNO order[=: ]+(\d{4,6})", "ORDER_SAVED"),
        (r"(?i)esim.*?orderId[=: ]+(\d{4,6})", "ESIM"),
        (r"(?:Starting|Processing) refund.*?orderId[=: ]+(\d{4,6})", "REFUND_INIT"),
        (r"Refund processed successfully.*?orderId[=: ]+(\d{4,6})", "REFUND_DONE"),
        (r"Successfully completed refund.*?orderId[=: ]+(\d{4,6})", "REFUND_DONE"),
        (r"RefundRequest.*?correlatorId[=](\d{4,6})", "REFUND_DETAIL"),
        (r"FB tracking data received from frontend.*?orderId[=](\d{4,6})", "FB_TRACKING"),
        (r"Creating new Stripe Customer.*?individualId[=: ]+([0-9a-f]{24})", "CUSTOMER_INFO"),
        (r"Got user info from certification.*?userId[=: ]+([0-9a-f-]{36})", "CUSTOMER_INFO"),
        (r"Detected sign-in provider[=: ]+(\S+?) for email[=: ]+(\S+)", "SIGN_IN_PROVIDER"),
        (r"(?:ERROR|WARN|Exception|failed|failure).*?orderId[=: ]+(\d{4,6})", "ERROR"),
    ]

    def __init__(self):
        self.client = LokiClient()
        self.db_client = DatabricksClient()
        self.latency_monitor = LatencyMonitor()
        self.orders = {}
        self.seen_hashes = set()
        self.last_poll = None
        self.poll_count = 0
        self.alerts = []

    def poll(self, is_initial=False):
        now = now_utc()
        if is_initial:
            start = now - timedelta(minutes=ORDER_CONFIG["initial_lookback_minutes"])
        else:
            start = (self.last_poll - timedelta(seconds=10)) if self.last_poll else \
                    now - timedelta(minutes=ORDER_CONFIG["lookback_minutes"])
        end = now
        ns = ORDER_CONFIG["namespace"]

        queries = [
            f'{{namespace="{ns}", container="rockstar-cj"}} |~ "OrderEventServiceImpl"',
            f'{{namespace="{ns}", container="rockstar-cj"}} |~ "orderId"',
            f'{{namespace="{ns}", container="rockstar-cj"}} |~ "(?i)refund"',
            f'{{namespace="{ns}", container="rockstar-cj"}} |~ "(?:FB tracking data|Creating new Stripe Customer|Got user info from certification|sign-in provider)"',
        ]

        all_logs = []
        query_errors = []
        for logql in queries:
            try:
                results = self.client.query(logql, start, end, 1000)
                for ts_ns, labels, line in results:
                    all_logs.append((ts_ns, labels, line))
            except Exception as e:
                query_errors.append(str(e))

        new_log_events = []
        for ts_ns, labels, line in all_logs:
            h = hash(line[:250])
            if h in self.seen_hashes:
                continue
            self.seen_hashes.add(h)
            ts_dt = datetime.utcfromtimestamp(ts_ns / 1e9)
            new_log_events.append((ts_dt, labels, line))

        new_log_events.sort(key=lambda x: x[0])

        processed_events = []
        for err in query_errors:
            processed_events.append((now, "\u2014", "ERROR", f"QUERY_ERROR: {err}"))

        for ts, labels, line in new_log_events:
            for pattern, event_type in self.PATTERNS:
                m = re.search(pattern, line)
                if not m:
                    continue

                if event_type == "STATE_CHANGE":
                    oid = m.group(1); new_state = m.group(2)
                elif event_type == "STATE_PROCESSING":
                    new_state = m.group(1); oid = m.group(2)
                elif event_type == "CUSTOMER_INFO":
                    cust_name_m = re.search(r"name[=: ]+([A-Z][A-Za-z .'-]+?)(?:,\s*(?:phone|type|email|contact)|$)", line)
                    cust_email_m = re.search(r"email[=: ]+([\w.+%-]+@[\w.-]+)", line)
                    ind_m = re.search(r"individualId[=: ]+([0-9a-f]{24})", line)
                    if ind_m:
                        for r in self.orders.values():
                            if r.individual_id == ind_m.group(1):
                                if cust_name_m and not r.customer_name:
                                    r.customer_name = cust_name_m.group(1).strip()
                                if cust_email_m and not r.customer_email:
                                    r.customer_email = cust_email_m.group(1)
                    break
                elif event_type == "SIGN_IN_PROVIDER":
                    provider = m.group(1); email = m.group(2)
                    for r in self.orders.values():
                        if r.customer_email == email or r.individual_id:
                            if not r.sign_in_provider: r.sign_in_provider = provider
                            if not r.customer_email: r.customer_email = email
                    break
                else:
                    oid = m.group(1); new_state = None

                rec = self.orders.get(oid)
                if rec is None:
                    rec = OrderRecord(oid)
                    rec.first_seen = ts
                    self.orders[oid] = rec
                if ts < rec.first_seen:
                    rec.first_seen = ts
                if rec.last_activity is None or ts > rec.last_activity:
                    rec.last_activity = ts
                changed = False

                if event_type == "STATE_CHANGE":
                    has_pay = "hasPaymentInfo=true" in line
                    old_state = rec.state
                    rec.set_state(ts, new_state, has_pay)
                    if old_state != rec.state:
                        pay_str = " (payment confirmed)" if has_pay else ""
                        rec.events.append((ts, f"State -> {new_state}{pay_str}"))
                        changed = True

                elif event_type == "STATE_PROCESSING":
                    if rec.state is None:
                        rec.set_state(ts, new_state)
                        rec.events.append((ts, f"State: {new_state} (processing)"))
                        changed = True

                elif event_type == "PAYMENT_PAGE":
                    if not rec.payment_page_loaded_at:
                        rec.payment_page_loaded_at = ts
                        am = re.search(r"amount[=: ]+(\d+)", line)
                        if am:
                            cents = int(am.group(1))
                            rec.amount = f"{cents/100:.2f}"
                        rec.events.append((ts, f"Payment page loaded (${rec.amount or '?'})"))
                        changed = True

                elif event_type == "INTENT_SAVED":
                    if not rec.payment_intent_saved_at:
                        rec.payment_intent_saved_at = ts
                        pi_m = re.search(r"paymentIntentId[=](pi_\w+)", line)
                        if pi_m: rec.payment_intent_id = pi_m.group(1)
                        rec.events.append((ts, f"Intent saved: {rec.payment_intent_id or '?'}"))
                        changed = True

                elif event_type == "ORDER_INFO":
                    am = re.search(r"amount[=](\d+\.\d+)\s*(\w+)", line)
                    if am:
                        rec.amount = am.group(1); rec.currency = am.group(2)
                    pm = re.search(r"productName[=](.+?)(?:\n|$)", line)
                    if pm: rec.product_name = pm.group(1).strip()

                elif event_type == "CONNECTX":
                    if not rec.connectx_at:
                        rec.connectx_at = ts
                        rec.events.append((ts, "ConnectX submitted"))
                        changed = True

                elif event_type == "MILESTONE":
                    if not rec.milestone_at:
                        rec.milestone_at = ts
                        rec.events.append((ts, "Milestone updated"))
                        changed = True

                elif event_type in ("ORDER_EXISTS", "ORDER_SAVED"):
                    if not rec.became_draft_at and rec.state is None:
                        rec.state = "draft"
                        rec.became_draft_at = ts
                        rec.events.append((ts, "Order exists/saved"))
                        changed = True

                elif event_type == "ESIM":
                    if not rec.esim_at:
                        rec.esim_at = ts
                        rec.events.append((ts, "eSIM activity"))
                        changed = True

                elif event_type == "REFUND_INIT":
                    if not rec.refund_initiated_at:
                        rec.refund_initiated_at = ts
                        amt_m = re.search(r"amount[=: ]+([\d.]+)\s*(\w{3})?", line)
                        if amt_m:
                            rec.refund_amount = amt_m.group(1)
                            if amt_m.group(2): rec.refund_currency = amt_m.group(2)
                        pid_m = re.search(r"paymentId[=: ]+([0-9a-f]{24})", line)
                        if pid_m: rec.refund_payment_id = pid_m.group(1)
                        rec.events.append((ts, f"Refund initiated ${rec.refund_amount or '?'}"))
                        changed = True

                elif event_type == "REFUND_DONE":
                    if not rec.refund_completed_at:
                        rec.refund_completed_at = ts
                        pid_m = re.search(r"paymentId[=: ]+([0-9a-f]{24})", line)
                        if pid_m and not rec.refund_payment_id:
                            rec.refund_payment_id = pid_m.group(1)
                        rec.events.append((ts, f"Refund completed ${rec.refund_amount or '?'}"))
                        changed = True

                elif event_type == "REFUND_DETAIL":
                    amt_m = re.search(r"totalAmount=PaymentAmount\(unit=(\w+),\s*value=([\d.]+)\)", line)
                    if amt_m and not rec.refund_amount:
                        rec.refund_currency = amt_m.group(1)
                        rec.refund_amount = amt_m.group(2)
                    pid_m = re.search(r"payment=RefundRequest\.PaymentReference\(id=([0-9a-f]{24})", line)
                    if pid_m and not rec.refund_payment_id:
                        rec.refund_payment_id = pid_m.group(1)
                    if not rec.refund_initiated_at:
                        rec.refund_initiated_at = ts
                        rec.events.append((ts, f"Refund initiated ${rec.refund_amount or '?'}"))
                        changed = True

                elif event_type == "FB_TRACKING":
                    ua_m = re.search(r"clientUserAgent[=](.+?)(?:\n|$)", line)
                    if ua_m and not rec.user_agent:
                        raw_ua = ua_m.group(1).strip()
                        rec.user_agent = raw_ua
                        plat, dev, ver = parse_user_agent(raw_ua)
                        if plat and not rec.platform: rec.platform = plat
                        if dev and not rec.device_model: rec.device_model = dev
                        if ver and not rec.app_version: rec.app_version = ver
                        rec.events.append((ts, f"Platform: {rec.platform or '?'} | {rec.device_model or '?'}"))
                        changed = True

                elif event_type == "ERROR":
                    rec.error_count += 1
                    err_m = re.search(r'(?:ERROR|Exception|failed)[: ]+(.{5,80}?)(?:\n|$)', line)
                    if err_m:
                        rec.last_error = err_m.group(1).strip()[:60]
                    rec.events.append((ts, f"Error: {rec.last_error or 'unknown'}"))
                    changed = True

                # Extract additional info from any matching line
                ind_m = re.search(r"individualId[=: ]+([0-9a-f]{24})", line)
                if ind_m and not rec.individual_id:
                    rec.individual_id = ind_m.group(1)
                ba_m = re.search(r"BillingAccountId.*?([0-9a-f]{24})", line)
                if ba_m and not rec.billing_account_id:
                    rec.billing_account_id = ba_m.group(1)
                if not rec.customer_email:
                    em = re.search(r"email[=: ]+([\w.+%-]+@[\w.-]+)", line)
                    if em: rec.customer_email = em.group(1)
                if not rec.customer_name:
                    nm_json = re.search(r'"name"\s*:\s*"([^"]+)"', line)
                    if nm_json:
                        rec.customer_name = nm_json.group(1)
                    else:
                        nm = re.search(r"(?:name[=: ]+|formattedName=)([A-Z][A-Za-z .'-]+?)(?:,\s*(?:type|phone|email|contact|@|role)|$)", line)
                        if nm:
                            name_val = nm.group(1).strip().strip('"')
                            if name_val and len(name_val) > 1 and not name_val.startswith("http"):
                                rec.customer_name = name_val
                if not rec.sign_in_provider:
                    sp = re.search(r"sign-in provider[=: ]+(\S+)", line)
                    if sp: rec.sign_in_provider = sp.group(1)

                if changed:
                    processed_events.append((ts, oid, event_type, line[:120]))
                break

        # Cross-link customer info among orders with same individualId
        by_ind = {}
        for r in self.orders.values():
            if r.individual_id:
                by_ind.setdefault(r.individual_id, []).append(r)
        for ind_id, recs in by_ind.items():
            name = next((r.customer_name for r in recs if r.customer_name), None)
            email = next((r.customer_email for r in recs if r.customer_email), None)
            provider = next((r.sign_in_provider for r in recs if r.sign_in_provider), None)
            ua = next((r.user_agent for r in recs if r.user_agent), None)
            plat = next((r.platform for r in recs if r.platform), None)
            dev = next((r.device_model for r in recs if r.device_model), None)
            ver = next((r.app_version for r in recs if r.app_version), None)
            for r in recs:
                if name and not r.customer_name: r.customer_name = name
                if email and not r.customer_email: r.customer_email = email
                if provider and not r.sign_in_provider: r.sign_in_provider = provider
                if ua and not r.user_agent: r.user_agent = ua
                if plat and not r.platform: r.platform = plat
                if dev and not r.device_model: r.device_model = dev
                if ver and not r.app_version: r.app_version = ver

        # Alert immediately when order moves to inProgress
        new_alerts = []
        for oid, rec in self.orders.items():
            if rec.should_alert():
                rec.alert_fired = True

                validation_summary = ""
                if self.db_client.is_enabled:
                    try:
                        v = self.db_client.get_order_validation(oid)
                        validation_summary = self.db_client.format_validation_summary(oid, v)
                        rec.last_select_number_type = v.get("select_number_type")
                        order_log(f"Order #{oid} Databricks validation: "
                            f"number_type={v.get('select_number_type')}, "
                            f"plans={v.get('plan_count')}, "
                            f"payment={v.get('payment_status')}, "
                            f"refund={'YES: ' + str(v.get('refund_status')) + ' $' + str(v.get('refund_amount')) if v.get('has_refund') else 'None'}, "
                            f"can_move={v.get('can_move')}")
                    except Exception as ex:
                        validation_summary = (
                            "=" * 50 + "\n"
                            "PRE-MOVE VALIDATION (Databricks mvno_order)\n"
                            "=" * 50 + "\n"
                            f"\u26a0\ufe0f  Could not query Databricks: {ex}\n"
                            "  Please manually check:\n"
                            f"  SELECT select_number_type FROM mvno_order WHERE order_id = '{oid}';\n"
                            "=" * 50
                        )
                        order_log(f"Order #{oid} Databricks validation failed: {ex}", "WARN")
                else:
                    validation_summary = (
                        "=" * 50 + "\n"
                        "PRE-MOVE VALIDATION (Databricks mvno_order)\n"
                        "=" * 50 + "\n"
                        "\u26a0\ufe0f  Databricks not configured. Please manually check:\n"
                        f"  SELECT select_number_type FROM mvno_order WHERE order_id = '{oid}';\n"
                        "  Possible values: NONE (do NOT move), NEW_NUMBER (ok), PORTIN (ok)\n"
                        "  Also verify: single plan + payment tab validated.\n"
                        "=" * 50
                    )

                # Fetch top 5 latency data for inclusion in alert
                latency_summary = ""
                try:
                    if self.latency_monitor.is_enabled:
                        _, top_n, _ = self.latency_monitor.poll()
                        if top_n:
                            latency_summary = self.latency_monitor.format_top5_for_order_alert(top_n)
                            order_log(f"Order #{oid} Top latency: {top_n[0]['duration_ms']:,}ms — {top_n[0]['api_name'][:40]}")
                except Exception as ex:
                    latency_summary = (
                        "=" * 50 + "\n"
                        "⏱️  TOP 5 SLOWEST API CALLS\n"
                        "=" * 50 + "\n"
                        f"⚠️  Could not fetch latency data: {ex}\n"
                        "=" * 50
                    )
                    order_log(f"Order #{oid} Latency fetch failed: {ex}", "WARN")

                msg = (
                    f"Order #{oid} -- PAYMENT CONFIRMED (InProgress)\n"
                    f"Customer: {rec.customer_name or '-'}\n"
                    f"Email: {rec.customer_email or '-'}\n"
                    f"Payment at: {fmt_ist(rec.became_inprogress_at)}\n"
                    f"Amount: ${rec.amount or '?'} {rec.currency or ''}\n"
                    f"Product: {rec.product_name or '-'}\n"
                    f"Platform: {rec.platform or '-'}\n"
                    f"Payment Intent: {rec.payment_intent_id or 'unknown'}\n"
                    f"Individual ID: {rec.individual_id or '-'}\n"
                    f"\n{validation_summary}"
                )
                if latency_summary:
                    msg += f"\n{latency_summary}"
                alert = (now_utc(), oid, msg)
                self.alerts.append(alert)
                new_alerts.append(alert)
                rec.events.append((now_utc(), "ALERT -- Payment confirmed, order InProgress"))

        # Re-check: Detect select_number_type CHANGES for alerted orders
        if self.db_client.is_enabled and not is_initial:
            for oid, rec in self.orders.items():
                if not rec.alert_fired:
                    continue
                if not rec.is_in_progress:
                    continue
                if rec.last_select_number_type is None:
                    try:
                        v = self.db_client.get_order_validation(oid)
                        rec.last_select_number_type = v.get("select_number_type")
                    except Exception:
                        pass
                    continue

                try:
                    sql = (
                        f"SELECT select_number_type "
                        f"FROM `{self.db_client.catalog}`.`{self.db_client.schema}`.mvno_order "
                        f"WHERE order_id = '{oid}'"
                    )
                    rows, err = self.db_client.execute_sql(sql)
                    if err or not rows:
                        continue
                    current_snt = rows[0].get("select_number_type", None)
                except Exception:
                    continue

                old_snt = rec.last_select_number_type
                if current_snt and current_snt != old_snt:
                    rec.select_number_type_history.append((now_utc(), old_snt, current_snt))
                    rec.last_select_number_type = current_snt

                    if current_snt in ("NEW_NUMBER", "PORT_IN", "PORTIN"):
                        verdict_icon = "\u2705"; verdict_text = "OK TO MOVE (mark MSISDN as Done)"
                    elif current_snt == "NONE":
                        verdict_icon = "\u274c"; verdict_text = "DO NOT MOVE \u2014 Customer has not selected any option"
                    else:
                        verdict_icon = "\u26a0\ufe0f"; verdict_text = f"Unexpected value: {current_snt}"

                    old_icon = "\u2705" if old_snt in ("NEW_NUMBER", "PORT_IN", "PORTIN") else ("\u274c" if old_snt == "NONE" else "\u26a0\ufe0f")
                    new_icon = "\u2705" if current_snt in ("NEW_NUMBER", "PORT_IN", "PORTIN") else ("\u274c" if current_snt == "NONE" else "\u26a0\ufe0f")

                    refund_line = "  Refund:   \u2705 No refund"
                    try:
                        v_change = self.db_client.get_order_validation(oid)
                        if v_change.get("has_refund"):
                            rs = v_change.get("refund_status", "UNKNOWN")
                            ra = v_change.get("refund_amount", "?")
                            rc = v_change.get("refund_currency", "USD")
                            rt = v_change.get("refund_type", "")
                            if rs and rs.upper() in ("PENDING", "PROCESSING"):
                                ref_icon = "\u26a0\ufe0f"
                            elif rs and rs.upper() in ("COMPLETED", "SUCCEEDED", "REFUNDED"):
                                ref_icon = "\u274c"
                                verdict_icon = "\u274c"
                                verdict_text = "DO NOT MOVE \u2014 Order has been refunded"
                            else:
                                ref_icon = "\u26a0\ufe0f"
                            refund_line = f"  Refund:   {ref_icon} {rs} \u2014 ${ra} {rc} ({rt})"
                    except Exception:
                        refund_line = "  Refund:   \u26a0\ufe0f Could not check"

                    change_msg = (
                        f"\U0001f504 NUMBER TYPE CHANGED \u2014 Order #{oid}\n"
                        f"{'=' * 50}\n"
                        f"  BEFORE:  {old_icon} {old_snt}\n"
                        f"  AFTER:   {new_icon} {current_snt}\n"
                        f"{'=' * 50}\n"
                        f"  Customer: {rec.customer_name or '-'}\n"
                        f"  Email:    {rec.customer_email or '-'}\n"
                        f"  Amount:   ${rec.amount or '?'} {rec.currency or ''}\n"
                        f"  Product:  {rec.product_name or '-'}\n"
                        f"{refund_line}\n"
                        f"  Changed:  {fmt_ist(now_utc())}\n"
                        f"{'=' * 50}\n"
                        f"  {verdict_icon} VERDICT: {verdict_text}\n"
                        f"{'=' * 50}"
                    )

                    change_alert = (now_utc(), oid, change_msg)
                    self.alerts.append(change_alert)
                    new_alerts.append(change_alert)
                    rec.events.append((now_utc(), f"ALERT -- Number type changed: {old_snt} -> {current_snt}"))
                    rec.number_type_change_alert_fired = True
                    order_log(f"NUMBER TYPE CHANGE Order #{oid}: {old_snt} -> {current_snt}", "ALERT")

        # Anomaly: Order stuck in DRAFT but payment already confirmed
        if not is_initial:
            for oid, rec in self.orders.items():
                if rec.draft_payment_alert_fired and not FORCE_ALERT:
                    continue
                if rec.state != "draft":
                    continue
                # Check payment from Loki logs first
                payment_confirmed = rec.has_payment_info
                # Also check Databricks if available
                if not payment_confirmed and self.db_client.is_enabled:
                    try:
                        v = self.db_client.get_order_validation(oid)
                        if v.get("payment_validated"):
                            payment_confirmed = True
                    except Exception:
                        pass
                if not payment_confirmed:
                    continue
                # Draft + payment confirmed = anomaly
                rec.draft_payment_alert_fired = True
                anomaly_msg = (
                    f"\U0001f534 DRAFT ORDER WITH PAYMENT — Order #{oid}\n"
                    f"{'=' * 50}\n"
                    f"  Status:   DRAFT (not moved to inProgress)\n"
                    f"  Payment:  CONFIRMED\n"
                    f"{'=' * 50}\n"
                    f"  Customer: {rec.customer_name or '-'}\n"
                    f"  Email:    {rec.customer_email or '-'}\n"
                    f"  Amount:   ${rec.amount or '?'} {rec.currency or ''}\n"
                    f"  Product:  {rec.product_name or '-'}\n"
                    f"  Draft at: {fmt_ist(rec.became_draft_at) if rec.became_draft_at else '-'}\n"
                    f"{'=' * 50}\n"
                    f"  \u26a0\ufe0f ACTION REQUIRED: Order has payment but is stuck in DRAFT.\n"
                    f"{'=' * 50}\n"
                    f"  FIX STEPS:\n"
                    f"  1. Open the Payment section\n"
                    f"  2. Scroll down — you will see 2 payments\n"
                    f"  3. Go to Action > Link Billing Account\n"
                    f"     on the SECOND payment\n"
                    f"  4. Open Link Billing Account:\n"
                    f"     - Unselect the one that is already selected\n"
                    f"     - Select the one that was unselected\n"
                    f"  5. Click Confirm\n"
                    f"{'=' * 50}"
                )
                anomaly_alert = (now_utc(), oid, anomaly_msg)
                self.alerts.append(anomaly_alert)
                new_alerts.append(anomaly_alert)
                rec.events.append((now_utc(), "ALERT -- Draft order with payment confirmed"))
                order_log(f"ANOMALY: Order #{oid} is DRAFT but has payment confirmed!", "ALERT")

        self.last_poll = end
        self.poll_count += 1
        return processed_events, new_alerts


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║             SECTION 1F — ORDER ALERT SENDERS (PATCHED: RETRY)            ║
# ╚════════════════════════════════════════════════════════════════════════════╝

class OrderSlackSender:
    _last_sent = {}
    SEND_MAX_RETRIES = 3  # PATCH v6.1: retry on failure

    @classmethod
    def send(cls, order_id, alert_msg):
        if not ORDER_SLACK_CONFIG.get("enabled") or not ORDER_SLACK_CONFIG.get("webhook_url"):
            return False, "Slack disabled or no webhook URL"
        # PATCH v6.1: Skip cooldown if --force-alert
        if not FORCE_ALERT:
            last = cls._last_sent.get(order_id)
            if last and (datetime.utcnow() - last).total_seconds() < ORDER_SLACK_CONFIG["cooldown_seconds"]:
                return False, "Slack cooldown active"

        # Build attachments-based payload (compatible with incoming webhooks).
        # Block Kit caused HTTP 400 errors with incoming webhooks.
        alert_text = alert_msg[:2900]  # Slack field limit with margin
        payload = {
            "attachments": [{
                "color": "#FF0000",
                "pretext": f"\U0001f6a8 *ORDER PAYMENT ALERT — Order #{order_id}*",
                "text": f"*Order #{order_id}* — Payment confirmed!\n{fmt_ist(now_utc())}",
                "fields": [
                    {"title": "\U0001f4cb Order Details",
                     "value": f"```{alert_text}```", "short": False},
                ],
                "footer": f"Unified Monitor v6.5 | {ORDER_CONFIG['namespace']}",
                "ts": int(time.time()),
            }],
        }

        last_error = None
        for attempt in range(1, cls.SEND_MAX_RETRIES + 1):
            try:
                payload_bytes = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(
                    ORDER_SLACK_CONFIG["webhook_url"], data=payload_bytes,
                    headers={"Content-Type": "application/json"}, method="POST",
                )
                ctx = ssl.create_default_context()
                with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                    resp_body = resp.read().decode("utf-8", errors="replace")
                    if resp.status == 200:
                        cls._last_sent[order_id] = datetime.utcnow()
                        return True, "Slack alert sent!"
                    else:
                        last_error = f"Slack HTTP {resp.status}: {resp_body[:100]}"
                        order_log(f"  Slack attempt {attempt}/{cls.SEND_MAX_RETRIES} failed: {last_error}", "ERROR")
            except urllib.error.HTTPError as e:
                body = ""
                try: body = e.read().decode("utf-8")[:200]
                except: pass
                last_error = f"Slack HTTP {e.code}: {body}"
                order_log(f"  Slack attempt {attempt}/{cls.SEND_MAX_RETRIES} failed: {last_error}", "ERROR")
            except Exception as e:
                last_error = f"Slack error: {e}"
                order_log(f"  Slack attempt {attempt}/{cls.SEND_MAX_RETRIES} failed: {last_error}", "ERROR")

            if attempt < cls.SEND_MAX_RETRIES:
                time.sleep(2 * attempt)  # backoff: 2s, 4s

        # PATCH v6.1: loud failure log
        order_log(f"  !!! SLACK DELIVERY FAILED after {cls.SEND_MAX_RETRIES} retries for order #{order_id}: {last_error}", "ERROR")
        return False, f"Slack failed after {cls.SEND_MAX_RETRIES} retries: {last_error}"


class EmailAlertSender:
    _last_sent = {}
    SEND_MAX_RETRIES = 2  # PATCH v6.1: retry on failure

    @classmethod
    def send(cls, order_id, alert_msg):
        if not EMAIL_CONFIG.get("enabled") or not EMAIL_CONFIG.get("smtp_password"):
            # PATCH v6.1: log WHY email is disabled
            reason = "Email disabled" if not EMAIL_CONFIG.get("enabled") else "No SMTP_PASSWORD env var set"
            order_log(f"  Email skipped: {reason}", "WARN")
            return False, reason
        # PATCH v6.1: Skip cooldown if --force-alert
        if not FORCE_ALERT:
            last = cls._last_sent.get(order_id)
            if last and (datetime.utcnow() - last).total_seconds() < EMAIL_CONFIG["cooldown_seconds"]:
                return False, "Cooldown active"

        last_error = None
        for attempt in range(1, cls.SEND_MAX_RETRIES + 1):
            try:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = f"{EMAIL_CONFIG['subject_prefix']} Order #{order_id} PAYMENT CONFIRMED"
                msg["From"] = EMAIL_CONFIG["from"]
                msg["To"] = EMAIL_CONFIG["to"]
                html = f"""
                <html><body style="background:#0F172A;color:#E2E8F0;font-family:Consolas,monospace;padding:20px">
                <div style="background:#7F1D1D;border:2px solid #DC2626;border-radius:8px;padding:16px;margin-bottom:20px">
                    <h2 style="color:#FCA5A5;margin:0">\U0001f6a8 ORDER PAYMENT ALERT</h2>
                    <p style="color:#FCA5A5;margin:4px 0 0">{fmt_ist(now_utc())}</p>
                </div>
                <pre style="background:#1E293B;padding:16px;border-radius:8px;color:#E2E8F0;font-size:14px">{alert_msg}</pre>
                </body></html>"""
                msg.attach(MIMEText(alert_msg, "plain"))
                msg.attach(MIMEText(html, "html"))
                with smtplib.SMTP(EMAIL_CONFIG["smtp_host"], EMAIL_CONFIG["smtp_port"]) as s:
                    if EMAIL_CONFIG["use_tls"]: s.starttls()
                    s.login(EMAIL_CONFIG["smtp_user"], EMAIL_CONFIG["smtp_password"])
                    s.send_message(msg)
                cls._last_sent[order_id] = datetime.utcnow()
                return True, f"Email sent to {EMAIL_CONFIG['to']}"
            except Exception as e:
                last_error = f"Email error: {e}"
                order_log(f"  Email attempt {attempt}/{cls.SEND_MAX_RETRIES} failed: {last_error}", "ERROR")
                if attempt < cls.SEND_MAX_RETRIES:
                    time.sleep(3)

        order_log(f"  !!! EMAIL DELIVERY FAILED after {cls.SEND_MAX_RETRIES} retries for order #{order_id}: {last_error}", "ERROR")
        return False, f"Email failed after {cls.SEND_MAX_RETRIES} retries: {last_error}"


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║       SECTION 1G — LOKI ERROR PATTERN MONITOR (mc-customer-journey-prod) ║
# ╚════════════════════════════════════════════════════════════════════════════╝

def error_log(msg, level="INFO"):
    ts = fmt_ist(now_utc())
    print(f"[{ts}] [ERROR-PATTERN] [{level}] {msg}", flush=True)


class LokiErrorPatternMonitor:
    """
    Polls Loki for error/exception/failure patterns in mc-customer-journey-prod
    (container=rockstar-cj) and sends Slack alerts on threshold breaches or spikes.
    """

    # Patterns to extract API endpoint / URL from log lines
    API_URL_PATTERNS = [
        re.compile(r'crawl url is\s+(https?://[^\s]+)', re.IGNORECASE),
        re.compile(r'Final request URL:\s+(https?://[^\s]+)', re.IGNORECASE),
        re.compile(r'Request URL:\s+(https?://[^\s]+)', re.IGNORECASE),
        re.compile(r'(?:GET|POST|PUT|DELETE|PATCH)\s+(https?://[^\s]+)', re.IGNORECASE),
        re.compile(r'url[=:]\s*(https?://[^\s,]+)', re.IGNORECASE),
    ]

    # Patterns to extract API name from step descriptions
    API_NAME_PATTERNS = [
        re.compile(r'\[Step\s+[\d.]+\]\s+(.+?)(?:,\s+time:|$)'),
        re.compile(r'([\w\s]+?)\s+API call'),
        re.compile(r'calling\s+([\w\s.-]+)', re.IGNORECASE),
    ]

    # 5xx status code pattern
    HTTP_5XX_PATTERN = re.compile(r'\b(500|502|503|504)\b')

    # Domain → friendly name (mirrors LATENCY_MONITOR_CONFIG.external_api_map)
    EXTERNAL_API_MAP = {
        "prod-boss-api.rockstar-automations.com": "Boss API",
        "billing.amdocs-dbs.com": "Amdocs Billing (ConnectX)",
        "account-management.amdocs-dbs.com": "Amdocs Account Mgmt (ConnectX)",
        "productcatalog.amdocs-dbs.com": "Amdocs Product Catalog (ConnectX)",
        "shoppingcart.amdocs-dbs.com": "Amdocs Shopping Cart (ConnectX)",
        "product-inventory-api.amdocs-dbs.com": "Amdocs Product Inventory (ConnectX)",
        "esim-adapter.amdocs-dbs.com": "Amdocs eSIM Adapter (ConnectX)",
        "party-management.amdocs-dbs.com": "Amdocs Party Mgmt (ConnectX)",
    }

    def __init__(self):
        self.client = LokiClient()
        self.cfg = LOKI_ERROR_PATTERN_CONFIG
        self.rolling_counts = []          # History of error counts per poll
        self.last_alert_time = {}         # pattern_key -> last alert datetime
        self.last_poll = None
        self.poll_count = 0
        self.total_errors_seen = 0

    @property
    def is_enabled(self):
        return self.cfg.get("enabled", False)

    def _build_logql(self):
        """Build LogQL query for error patterns."""
        ns = self.cfg["namespace"]
        app = self.cfg["app_label"]
        patterns = self.cfg["error_patterns"]
        return f'{{namespace="{ns}", container="{app}"}} |~ "(?i)({patterns})"'

    def _is_on_cooldown(self, pattern_key):
        last = self.last_alert_time.get(pattern_key)
        if not last:
            return False
        elapsed = (now_utc() - last).total_seconds()
        return elapsed < self.cfg["cooldown_seconds"]

    def _rolling_average(self):
        if not self.rolling_counts:
            return 0.0
        return sum(self.rolling_counts) / len(self.rolling_counts)

    def _extract_api_endpoint(self, line):
        """
        Extract API endpoint / friendly name from a log line.
        Returns a string like 'Amdocs Billing (ConnectX)' or 'unknown-host.com/path'
        or 'Unknown endpoint' if nothing can be extracted.
        """
        # Try to find a URL in the line
        for pat in self.API_URL_PATTERNS:
            m = pat.search(line)
            if m:
                url = m.group(1)
                try:
                    parsed = urlparse(url)
                    host = parsed.hostname or ""
                    # Check against known external API domains
                    for domain, friendly in self.EXTERNAL_API_MAP.items():
                        if domain in host:
                            path = parsed.path
                            if len(path) > 50:
                                path = path[:47] + "..."
                            return f"{friendly} ({path})" if path and path != "/" else friendly
                    # Unknown domain — use host + truncated path
                    path = parsed.path
                    if len(path) > 40:
                        path = path[:37] + "..."
                    return f"{host}{path}" if path and path != "/" else host
                except Exception:
                    return url[:80]

        # Try to extract API name from step descriptions
        for pat in self.API_NAME_PATTERNS:
            m = pat.search(line)
            if m:
                name = m.group(1).strip()
                if len(name) > 5:  # Skip very short matches
                    return name[:80]

        return "Unknown endpoint"

    def _categorize_errors(self, log_lines):
        """Group error lines by category for the alert summary."""
        categories = {
            "HTTP 5xx":     re.compile(r'\b(500|502|503|504)\b'),
            "HTTP 404":     re.compile(r'\b404\b'),
            "Timeout":      re.compile(r'(?i)\btimeout\b'),
            "Auth":         re.compile(r'(?i)\b(unauthorized|forbidden|denied)\b'),
            "Exception":    re.compile(r'(?i)\b(exception|crash|fatal|critical)\b'),
            "Failure":      re.compile(r'(?i)\b(failed|failure|refused)\b'),
            "Invalid/Null": re.compile(r'(?i)\b(invalid|null|undefined)\b'),
            "Warning":      re.compile(r'(?i)\bwarn\b'),
            "Error":        re.compile(r'(?i)\berror\b'),
        }
        counts = {cat: 0 for cat in categories}
        samples = {cat: [] for cat in categories}

        # --- 5xx-specific tracking: {status_code: {api_endpoint: {"count": N, "samples": [...]}}} ---
        fivexx_by_endpoint = {}

        for ts_ns, labels, line in log_lines:
            matched = False
            for cat, regex in categories.items():
                if regex.search(line):
                    counts[cat] += 1
                    if len(samples[cat]) < 3:  # Keep up to 3 samples per category
                        samples[cat].append(line[:200])
                    matched = True

                    # Extra tracking for 5xx errors: identify code + endpoint
                    if cat == "HTTP 5xx":
                        m5 = self.HTTP_5XX_PATTERN.search(line)
                        status_code = m5.group(1) if m5 else "5xx"
                        endpoint = self._extract_api_endpoint(line)

                        if status_code not in fivexx_by_endpoint:
                            fivexx_by_endpoint[status_code] = {}
                        ep_data = fivexx_by_endpoint[status_code]
                        if endpoint not in ep_data:
                            ep_data[endpoint] = {"count": 0, "samples": []}
                        ep_data[endpoint]["count"] += 1
                        if len(ep_data[endpoint]["samples"]) < 2:
                            ep_data[endpoint]["samples"].append(line[:200])

                    break  # Count in first matching category only
            if not matched:
                counts["Error"] += 1

        return counts, samples, fivexx_by_endpoint

    def _extract_order_details(self, log_lines):
        """Extract order-linked error details from log lines for the alert table.

        Returns a deduplicated list of dicts (one per order_id + error_type),
        keeping only the latest occurrence.
        """
        order_id_re = re.compile(r'orderId[=: ]+(\d{4,6})')
        email_re = re.compile(r'email[=: ]+([\w.+%-]+@[\w.-]+)')
        categories = {
            "HTTP 5xx":     re.compile(r'\b(500|502|503|504)\b'),
            "Timeout":      re.compile(r'(?i)\btimeout\b'),
            "Auth":         re.compile(r'(?i)\b(unauthorized|forbidden|denied)\b'),
            "Exception":    re.compile(r'(?i)\b(exception|crash|fatal|critical)\b'),
            "Failure":      re.compile(r'(?i)\b(failed|failure|refused)\b'),
            "Invalid/Null": re.compile(r'(?i)\b(invalid|null|undefined)\b'),
            "Error":        re.compile(r'(?i)\berror\b'),
        }

        # key = (order_id, error_type) -> dict with latest ts
        seen = {}

        for ts_ns, labels, line in log_lines:
            oid_m = order_id_re.search(line)
            if not oid_m:
                continue
            order_id = oid_m.group(1)

            # Determine error type
            error_type = "Error"
            for cat, regex in categories.items():
                if regex.search(line):
                    error_type = cat
                    break

            email_m = email_re.search(line)
            email = email_m.group(1) if email_m else "-"

            ts_dt = datetime.utcfromtimestamp(ts_ns / 1e9)
            ts_str = short_ist(ts_dt)

            key = (order_id, error_type)
            # Keep the latest occurrence per (order_id, error_type)
            if key not in seen or ts_ns > seen[key]["_ts_ns"]:
                seen[key] = {
                    "order_id": order_id,
                    "email": email,
                    "error_type": error_type,
                    "timestamp": ts_str,
                    "_ts_ns": ts_ns,
                }

        # Sort by timestamp descending (most recent first)
        rows = sorted(seen.values(), key=lambda r: r["_ts_ns"], reverse=True)
        return rows

    def poll(self):
        """
        Query Loki for error patterns and return (error_count, alerts_to_send).
        """
        now = now_utc()
        lookback = self.cfg["lookback_minutes"]
        start = now - timedelta(minutes=lookback)
        end = now

        logql = self._build_logql()

        try:
            results = self.client.query(logql, start, end, limit=500)
        except Exception as e:
            error_log(f"Loki query failed: {e}", "ERROR")
            return 0, []

        error_count = len(results)
        self.total_errors_seen += error_count
        self.poll_count += 1

        # Update rolling window
        self.rolling_counts.append(error_count)
        window = self.cfg["rolling_window_size"]
        if len(self.rolling_counts) > window:
            self.rolling_counts = self.rolling_counts[-window:]

        rolling_avg = self._rolling_average()
        spike_threshold = rolling_avg * self.cfg["spike_multiplier"]
        abs_threshold = self.cfg["alert_threshold"]

        self.last_poll = now

        alerts_to_send = []

        # Determine if we should alert
        should_alert = False
        alert_reason = ""

        if error_count >= abs_threshold:
            should_alert = True
            alert_reason = f"Threshold breach: {error_count} errors (threshold: {abs_threshold})"

        if self.poll_count > 3 and rolling_avg > 0 and error_count > spike_threshold:
            should_alert = True
            alert_reason = (
                f"Spike detected: {error_count} errors "
                f"(rolling avg: {rolling_avg:.1f}, spike threshold: {spike_threshold:.1f})"
            )

        if should_alert and not self._is_on_cooldown("error_pattern_alert"):
            # Categorize errors for a useful alert
            categories, samples, fivexx_by_endpoint = self._categorize_errors(results)
            alert_msg = self._build_alert_message(error_count, alert_reason, categories, samples, rolling_avg, fivexx_by_endpoint, log_lines=results)
            alerts_to_send.append(alert_msg)
            self.last_alert_time["error_pattern_alert"] = now
            error_log(f"ALERT: {alert_reason}", "ALERT")

        return error_count, alerts_to_send

    def _build_alert_message(self, error_count, reason, categories, samples, rolling_avg, fivexx_by_endpoint=None, log_lines=None):
        """Build a structured Slack alert payload."""
        ns = self.cfg["namespace"]
        app = self.cfg["app_label"]
        lookback = self.cfg["lookback_minutes"]

        # Category breakdown text
        breakdown_lines = []
        for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
            if count > 0:
                breakdown_lines.append(f"  \u2022 {cat}: {count}")
        breakdown = "\n".join(breakdown_lines) if breakdown_lines else "  (no categorization)"

        # --- 5xx by endpoint section ---
        fivexx_section = ""
        if fivexx_by_endpoint:
            fivexx_lines = []
            # Sort status codes numerically
            for code in sorted(fivexx_by_endpoint.keys()):
                endpoints = fivexx_by_endpoint[code]
                total_for_code = sum(ep["count"] for ep in endpoints.values())
                fivexx_lines.append(f"\n*`HTTP {code}`* \u2014 {total_for_code} total")
                # Sort endpoints by count descending
                for ep_name, ep_data in sorted(endpoints.items(), key=lambda x: -x[1]["count"]):
                    fivexx_lines.append(
                        f"    \u2022 *`{ep_name}`* \u2014 *{ep_data['count']}* hits"
                    )
                    # Show one sample line per endpoint
                    if ep_data["samples"]:
                        sample = ep_data["samples"][0].replace("`", "'").replace("```", "'''")[:150]
                        fivexx_lines.append(f"      `{sample}`")
            fivexx_section = "\n".join(fivexx_lines)

        # Sample error lines (non-5xx categories)
        sample_lines = []
        max_samples = self.cfg["max_log_lines_in_alert"]
        shown = 0
        for cat, lines in samples.items():
            if cat == "HTTP 5xx":
                continue  # 5xx samples are shown in the dedicated section
            for line in lines:
                if shown >= max_samples:
                    break
                # Truncate long lines and sanitize for Slack
                clean = line.replace("`", "'").replace("```", "'''")[:180]
                sample_lines.append(f"[{cat}] {clean}")
                shown += 1
            if shown >= max_samples:
                break
        sample_text = "\n".join(sample_lines) if sample_lines else "(no samples)"

        text_block = (
            f"*Namespace:* `{ns}`\n"
            f"*App:* `{app}`\n"
            f"*Window:* Last {lookback} min\n"
            f"*Error count:* {error_count}\n"
            f"*Rolling avg:* {rolling_avg:.1f}\n"
            f"*Reason:* {reason}\n\n"
            f"*Breakdown:*\n{breakdown}"
        )

        fields = []

        # Add 5xx-by-endpoint field if present
        if fivexx_section:
            fields.append({
                "title": "\U0001f534 5xx Errors by API Endpoint",
                "value": fivexx_section,
                "short": False,
            })

        # Add general sample lines
        fields.append({
            "title": "\U0001f4cb Sample Error Lines",
            "value": f"```{sample_text}```",
            "short": False,
        })

        # --- Affected Orders table ---
        if log_lines:
            order_rows = self._extract_order_details(log_lines)
            if order_rows:
                max_rows = self.cfg.get("max_log_lines_in_alert", 15)
                display_rows = order_rows[:max_rows]
                skipped_errors = error_count - len(order_rows)

                separator = "\u2500" * 53
                header = f" {'Order':<8}{'Email':<23}{'Error Type':<14}{'Time'}"
                table_lines = [separator, header, separator]
                for r in display_rows:
                    table_lines.append(
                        f" #{r['order_id']:<7}{r['email']:<23}{r['error_type']:<14}{r['timestamp']}"
                    )
                table_lines.append(separator)
                summary = f" {len(order_rows)} order(s) affected"
                if skipped_errors > 0:
                    summary += f" | {skipped_errors} errors without order ID skipped"
                table_lines.append(summary)
                if len(order_rows) > max_rows:
                    table_lines.append(f" (showing top {max_rows} of {len(order_rows)})")

                order_table_text = "\n".join(table_lines)
                fields.append({
                    "title": "\U0001f4cb Affected Orders",
                    "value": f"```{order_table_text}```",
                    "short": False,
                })

        return {
            "attachments": [{
                "color": "#FF4444",
                "pretext": f"\U0001f6a8 *Loki Error Pattern Alert \u2014 {app}*",
                "text": text_block,
                "fields": fields,
                "footer": f"Unified Monitor v6.5 | Loki Error Pattern | {ns}",
                "footer_icon": "https://img.icons8.com/color/48/000000/error--v1.png",
                "ts": int(time.time()),
            }],
        }

    def get_status_summary(self):
        """Return a one-line status summary for the banner."""
        if not self.is_enabled:
            return "OFF"
        return (
            f"ON | polls={self.poll_count}, total_errors={self.total_errors_seen}, "
            f"rolling_avg={self._rolling_average():.1f}"
        )


def run_error_pattern_monitor(running_flag):
    """Loki error pattern monitor main loop (runs in its own thread)."""
    monitor = LokiErrorPatternMonitor()
    if not monitor.is_enabled:
        error_log("Loki error pattern monitor is disabled.")
        return

    cfg = LOKI_ERROR_PATTERN_CONFIG
    error_log("=" * 60)
    error_log("Loki Error Pattern Monitor Started")
    error_log(f"  Namespace:  {cfg['namespace']}")
    error_log(f"  App:        {cfg['app_label']}")
    error_log(f"  Patterns:   {cfg['error_patterns'][:80]}...")
    error_log(f"  Interval:   {cfg['poll_interval_seconds']}s")
    error_log(f"  Lookback:   {cfg['lookback_minutes']}min")
    error_log(f"  Threshold:  {cfg['alert_threshold']} errors / spike {cfg['spike_multiplier']}x avg")
    error_log(f"  Cooldown:   {cfg['cooldown_seconds']}s")
    error_log("=" * 60)

    # Test Loki connection using existing client
    error_log("Testing Loki/Grafana connection...")
    ok, msg, method = monitor.client.test_connection()
    if ok:
        error_log(f"OK {msg}")
    else:
        error_log(f"FAIL {msg}", "ERROR")
        error_log("Cannot connect to Grafana for error pattern monitoring.", "ERROR")
        return

    # Initial poll
    try:
        count, alerts = monitor.poll()
        error_log(f"Initial scan: {count} error lines found in last {cfg['lookback_minutes']}min")
        for alert_payload in alerts:
            send_status_slack(alert_payload)
    except Exception as ex:
        error_log(f"Initial poll failed: {ex}", "ERROR")

    # Main loop
    while running_flag["active"]:
        for _ in range(cfg["poll_interval_seconds"] * 10):
            if not running_flag["active"]:
                break
            time.sleep(0.1)
        if not running_flag["active"]:
            break

        try:
            count, alerts = monitor.poll()
            if count > 0:
                error_log(
                    f"Poll #{monitor.poll_count}: {count} errors "
                    f"(avg: {monitor._rolling_average():.1f})"
                )
            else:
                error_log(f"Poll #{monitor.poll_count}: no errors")

            for alert_payload in alerts:
                ok = send_status_slack(alert_payload)
                if ok:
                    error_log("Slack error pattern alert sent!", "ALERT")
                else:
                    error_log("Slack error pattern alert FAILED!", "ERROR")

        except Exception as ex:
            error_log(f"Poll failed: {ex}", "ERROR")

    error_log("Loki error pattern monitor stopped.")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║       SECTION 1H — API LATENCY MONITOR (Top 5 Slow APIs)                ║
# ╚════════════════════════════════════════════════════════════════════════════╝

def latency_log(msg, level="INFO"):
    ts = fmt_ist(now_utc())
    print(f"[{ts}] [LATENCY] [{level}] {msg}", flush=True)


class LatencyMonitor:
    """
    Polls Loki for API call timing patterns in mc-customer-journey-prod
    (container=rockstar-cj), ranks them by latency, classifies as internal/external,
    and sends Slack alerts with top 5 slowest API calls when thresholds breach.
    """

    # Regex patterns to extract timing information from application logs
    TIMING_PATTERNS = [
        # [Step N] description, time: NNNNms
        re.compile(
            r'\[Step\s+([\d.]+)\]\s+(.+?),?\s+time:\s+(\d+)ms'
        ),
        # API call took: NNNNms, productIds: [...]
        re.compile(
            r'(.+?)\s+API call took:\s+(\d+)ms'
        ),
        # Cart creation completed, total time: NNNNms
        re.compile(
            r'(Cart creation completed|Order creation completed|.*completed).*?total\s+time:\s+(\d+)ms'
        ),
    ]

    # Regex to capture external API URLs from crawl/request logs
    URL_PATTERNS = [
        re.compile(r'crawl url is\s+(https?://[^\s]+)'),
        re.compile(r'Final request URL:\s+(https?://[^\s]+)'),
        re.compile(r'Request URL:\s+(https?://[^\s]+)'),
    ]

    def __init__(self):
        self.client = LokiClient()
        self.cfg = LATENCY_MONITOR_CONFIG
        self.last_alert_time = {}
        self.last_poll = None
        self.poll_count = 0
        self.total_slow_calls = 0
        self.rolling_latencies = []     # Track recent max latencies for trend detection

    @property
    def is_enabled(self):
        return self.cfg.get("enabled", False)

    def _is_on_cooldown(self, key):
        last = self.last_alert_time.get(key)
        if not last:
            return False
        return (now_utc() - last).total_seconds() < self.cfg["cooldown_seconds"]

    def _classify_api(self, step_desc, url=None):
        """
        Classify an API call as 'External' or 'Internal' and return a friendly name.
        Returns (type_label, friendly_name).
        Priority: 1) Internal patterns (fastest check), 2) URL domain map, 3) Heuristics
        """
        # FIRST: Check step description against known internal patterns
        for pattern in self.cfg.get("internal_patterns", []):
            if pattern.lower() in step_desc.lower():
                return "🏠 Internal", step_desc.strip()

        # Check for zero-duration (memory operations, always internal)
        if "memory" in step_desc.lower() or "0ms" in step_desc:
            return "🏠 Internal", step_desc.strip()

        # SECOND: Check URL against known external API map
        if url:
            for domain, friendly in self.cfg["external_api_map"].items():
                if domain in url:
                    try:
                        parsed = urlparse(url)
                        path = parsed.path
                        if parsed.query:
                            path += "?" + parsed.query[:30]
                        if len(path) > 60:
                            path = path[:57] + "..."
                        return "🌐 External", f"{friendly} ({path})"
                    except Exception:
                        return "🌐 External", friendly

            # Unknown external URL
            try:
                parsed = urlparse(url)
                host = parsed.hostname or url[:50]
                return "🌐 External", f"{host}"
            except Exception:
                return "🌐 External", url[:60]

        # THIRD: Heuristic classification based on step description keywords
        external_hints = [
            ("Boss API", "Boss API"),
            ("ConnectX", "Amdocs ConnectX"),
            ("Amdocs", "Amdocs ConnectX"),
            ("Catalog", "Product Catalog (ConnectX)"),
            ("BillingAccount", "Billing Account (ConnectX)"),
            ("coverage", "Service Qualification (Boss API)"),
            ("zip code coverage", "Service Qualification (Boss API)"),
            ("Create cart", "Shopping Cart (ConnectX)"),
            ("Batch get product", "Product Catalog (ConnectX)"),
            ("Create order", "Order API (ConnectX)"),
            ("eSIM", "eSIM Adapter (ConnectX)"),

        ]
        for hint, name in external_hints:
            if hint.lower() in step_desc.lower():
                return "🌐 External", f"{name} — {step_desc.strip()}"

        # Default: internal
        return "🏠 Internal", step_desc.strip()

    def _parse_timing_logs(self, log_lines):
        """
        Parse log lines and extract API call timing data.
        Returns list of dicts: [{step, description, duration_ms, api_type, api_name, url, timestamp, request_id}, ...]
        """
        timings = []
        # First pass: collect URL context (crawl url lines mapped by approximate timestamp)
        url_context = []
        for ts_ns, labels, line in log_lines:
            for url_pat in self.URL_PATTERNS:
                m = url_pat.search(line)
                if m:
                    url_context.append({
                        "ts_ns": ts_ns,
                        "url": m.group(1),
                        "line": line,
                    })
                    break

        # Second pass: extract timing entries
        for ts_ns, labels, line in log_lines:
            ts_dt = datetime.utcfromtimestamp(ts_ns / 1e9)

            # Pattern 1: [Step X] description, time: NNNNms
            m = self.TIMING_PATTERNS[0].search(line)
            if m:
                step_num = m.group(1)
                desc = m.group(2).strip()
                duration = int(m.group(3))

                # Try to find a URL call that happened just before this timing log
                associated_url = self._find_associated_url(ts_ns, url_context, desc)
                api_type, api_name = self._classify_api(desc, associated_url)

                # Extract request context (orderId, individualId, etc.)
                request_id = ""
                oid_m = re.search(r'orderId[=: ]+(\d+)', line)
                if oid_m:
                    request_id = f"order={oid_m.group(1)}"
                ind_m = re.search(r'individualId[=: ]+([0-9a-f]{24})', line)
                if ind_m:
                    request_id += f" ind={ind_m.group(1)[:12]}..."

                timings.append({
                    "step": f"Step {step_num}",
                    "description": desc,
                    "duration_ms": duration,
                    "api_type": api_type,
                    "api_name": api_name,
                    "url": associated_url or "",
                    "timestamp": ts_dt,
                    "request_id": request_id.strip(),
                    "pod": labels.get("pod", "unknown"),
                })
                continue

            # Pattern 2: ... API call took: NNNNms
            m = self.TIMING_PATTERNS[1].search(line)
            if m:
                desc = m.group(1).strip()
                duration = int(m.group(2))
                associated_url = self._find_associated_url(ts_ns, url_context, desc)
                api_type, api_name = self._classify_api(desc, associated_url)
                timings.append({
                    "step": "API Call",
                    "description": desc,
                    "duration_ms": duration,
                    "api_type": api_type,
                    "api_name": api_name,
                    "url": associated_url or "",
                    "timestamp": ts_dt,
                    "request_id": "",
                    "pod": labels.get("pod", "unknown"),
                })
                continue

            # Pattern 3: ... total time: NNNNms
            m = self.TIMING_PATTERNS[2].search(line)
            if m:
                desc = m.group(1).strip()
                duration = int(m.group(2))
                timings.append({
                    "step": "Total",
                    "description": desc,
                    "duration_ms": duration,
                    "api_type": "📊 Total",
                    "api_name": desc,
                    "url": "",
                    "timestamp": ts_dt,
                    "request_id": "",
                    "pod": labels.get("pod", "unknown"),
                })

        return timings

    def _find_associated_url(self, timing_ts_ns, url_context, step_desc):
        """
        Find a URL logged close in time (within 8s before) to a timing log entry.
        Uses the CLOSEST URL that appeared before the timing entry.
        Marks used URLs to prevent reuse by later timing entries.
        """
        best_url = None
        best_gap = float("inf")
        best_idx = -1

        for i, uc in enumerate(url_context):
            if uc.get("_used"):
                continue
            gap = (timing_ts_ns - uc["ts_ns"]) / 1e9  # seconds
            # URL should appear BEFORE the timing log (gap > 0) and within 8 seconds
            if 0 <= gap <= 8 and gap < best_gap:
                best_gap = gap
                best_url = uc["url"]
                best_idx = i

        # Mark the URL as consumed so it won't be reused
        if best_idx >= 0:
            url_context[best_idx]["_used"] = True

        return best_url

    def get_top_n_latencies(self, timings, n=5):
        """Return top N slowest API calls, excluding 'Total' entries and deduplicating."""
        non_total = [t for t in timings if t["api_type"] != "📊 Total"]

        # Deduplicate: if two entries have the same URL and very similar duration (within 5ms),
        # keep only the one with the "Step" label (more descriptive)
        deduped = []
        seen_keys = set()
        # Sort by duration desc so we process highest first
        sorted_by_dur = sorted(non_total, key=lambda x: x["duration_ms"], reverse=True)
        for t in sorted_by_dur:
            # Create a dedup key: URL + approximate duration bucket (within 50ms)
            url_key = t.get("url", "")
            dur_bucket = t["duration_ms"] // 50
            dedup_key = f"{url_key}|{dur_bucket}" if url_key else None

            if dedup_key and dedup_key in seen_keys:
                continue
            if dedup_key:
                seen_keys.add(dedup_key)
            deduped.append(t)

        return deduped[:n]

    def poll(self):
        """
        Query Loki for timing patterns and return (all_timings, top_n, alerts_to_send).
        """
        now = now_utc()
        lookback = self.cfg["lookback_minutes"]
        start = now - timedelta(minutes=lookback)
        end = now

        ns = self.cfg["namespace"]
        app = self.cfg["app_label"]

        # Query 1: Step timing logs
        logql_timing = f'{{namespace="{ns}", container="{app}"}} |~ "time:\\\\s+\\\\d+ms"'
        # Query 2: URL context logs (crawl url, Final request URL)
        logql_urls = f'{{namespace="{ns}", container="{app}"}} |~ "(?:crawl url|Final request URL|Request URL).*https?://"'
        # Query 3: Total time logs
        logql_total = f'{{namespace="{ns}", container="{app}"}} |~ "total\\\\s+time:\\\\s+\\\\d+ms"'
        # Query 4: API call took logs
        logql_api = f'{{namespace="{ns}", container="{app}"}} |~ "API call took"'

        all_logs = []
        for logql in [logql_timing, logql_urls, logql_total, logql_api]:
            try:
                results = self.client.query(logql, start, end, limit=500)
                all_logs.extend(results)
            except Exception as e:
                latency_log(f"Loki query failed: {e}", "ERROR")

        # Deduplicate by line hash
        seen = set()
        unique_logs = []
        for ts_ns, labels, line in all_logs:
            h = hash(line[:250])
            if h not in seen:
                seen.add(h)
                unique_logs.append((ts_ns, labels, line))

        unique_logs.sort(key=lambda x: x[0])

        timings = self._parse_timing_logs(unique_logs)
        top_n = self.get_top_n_latencies(timings, self.cfg["top_n"])

        self.poll_count += 1
        self.last_poll = now

        # Track rolling max latency
        if top_n:
            max_lat = top_n[0]["duration_ms"]
            self.rolling_latencies.append(max_lat)
            if len(self.rolling_latencies) > 20:
                self.rolling_latencies = self.rolling_latencies[-20:]

        alerts_to_send = []

        # Check thresholds for alerting
        slow_threshold = self.cfg["slow_threshold_ms"]
        total_threshold = self.cfg["total_threshold_ms"]

        # Find calls exceeding slow threshold
        slow_calls = [t for t in timings if t["duration_ms"] >= slow_threshold and t["api_type"] != "📊 Total"]
        # Find total request time exceeding threshold
        slow_totals = [t for t in timings if t["api_type"] == "📊 Total" and t["duration_ms"] >= total_threshold]

        if (slow_calls or slow_totals) and not self._is_on_cooldown("latency_alert"):
            self.total_slow_calls += len(slow_calls)
            alert_payload = self._build_alert(timings, top_n, slow_calls, slow_totals)
            alerts_to_send.append(alert_payload)
            self.last_alert_time["latency_alert"] = now
            latency_log(f"ALERT: {len(slow_calls)} slow API calls, {len(slow_totals)} slow total requests", "ALERT")

        return timings, top_n, alerts_to_send

    def _build_alert(self, all_timings, top_n, slow_calls, slow_totals):
        """Build a Slack alert payload for slow API calls."""
        ns = self.cfg["namespace"]
        app = self.cfg["app_label"]
        lookback = self.cfg["lookback_minutes"]

        # Build top 5 table
        top5_lines = []
        for i, t in enumerate(top_n, 1):
            duration_str = f"{t['duration_ms']:,}ms"
            pct_bar = "█" * min(int(t['duration_ms'] / 500), 20)
            top5_lines.append(
                f"  #{i}  {duration_str:>8}  {t['api_type']}  {t['api_name'][:55]}"
            )
            if t.get("url"):
                url_short = t["url"][:70] + ("..." if len(t["url"]) > 70 else "")
                top5_lines.append(f"             └─ {url_short}")
            if t.get("pod"):
                top5_lines.append(f"             └─ pod: {t['pod']}  {short_ist(t['timestamp'])}")
        top5_text = "\n".join(top5_lines) if top5_lines else "  No timing data found"

        # Summary stats
        durations = [t["duration_ms"] for t in all_timings if t["api_type"] != "📊 Total"]
        external_calls = [t for t in all_timings if "External" in t["api_type"]]
        internal_calls = [t for t in all_timings if "Internal" in t["api_type"]]

        avg_ms = sum(durations) / len(durations) if durations else 0
        max_ms = max(durations) if durations else 0
        ext_avg = sum(t["duration_ms"] for t in external_calls) / len(external_calls) if external_calls else 0
        int_avg = sum(t["duration_ms"] for t in internal_calls) / len(internal_calls) if internal_calls else 0

        summary_text = (
            f"*Namespace:* `{ns}`  |  *App:* `{app}`\n"
            f"*Window:* Last {lookback} min  |  *Total API calls tracked:* {len(all_timings)}\n"
            f"*🌐 External calls:* {len(external_calls)} (avg: {ext_avg:,.0f}ms)\n"
            f"*🏠 Internal calls:* {len(internal_calls)} (avg: {int_avg:,.0f}ms)\n"
            f"*Max latency:* {max_ms:,}ms  |  *Avg:* {avg_ms:,.0f}ms\n"
            f"*Slow calls (>{self.cfg['slow_threshold_ms']}ms):* {len(slow_calls)}"
        )

        # Slow total requests
        total_lines = []
        for t in slow_totals:
            total_lines.append(f"  ⏱️  {t['duration_ms']:,}ms — {t['description'][:60]}  (pod: {t['pod']})")
        total_text = "\n".join(total_lines) if total_lines else "  None exceeding threshold"

        return {
            "attachments": [{
                "color": "#FF6600",
                "pretext": f"⏱️ *API Latency Alert — Top {self.cfg['top_n']} Slowest Calls*",
                "text": summary_text,
                "fields": [
                    {
                        "title": f"🏆 Top {len(top_n)} Slowest API Calls",
                        "value": f"```{top5_text}```",
                        "short": False,
                    },
                    {
                        "title": "⏱️ Slow Total Request Times",
                        "value": f"```{total_text}```",
                        "short": False,
                    },
                ],
                "footer": f"Unified Monitor v6.5 | API Latency | {ns} | threshold: {self.cfg['slow_threshold_ms']}ms",
                "footer_icon": "https://img.icons8.com/color/48/000000/speed.png",
                "ts": int(time.time()),
            }],
        }

    def format_top5_for_order_alert(self, top_n):
        """
        Format the top 5 latency data as a plain-text block suitable for inclusion
        in the order payment alert message.
        """
        if not top_n:
            return ""
        lines = []
        lines.append("=" * 50)
        lines.append("⏱️  TOP 5 SLOWEST API CALLS (This Window)")
        lines.append("=" * 50)
        total_of_top5 = sum(t["duration_ms"] for t in top_n)
        for i, t in enumerate(top_n, 1):
            pct = (t["duration_ms"] / total_of_top5 * 100) if total_of_top5 > 0 else 0
            bar = "█" * max(1, int(pct / 5))
            lines.append(
                f"  #{i}  {t['duration_ms']:>6,}ms  ({pct:4.1f}%)  {bar}"
            )
            lines.append(
                f"       {t['api_type']}  {t['api_name'][:50]}"
            )
            if t.get("url"):
                url_short = t["url"][:65] + ("..." if len(t["url"]) > 65 else "")
                lines.append(f"       └─ {url_short}")
        lines.append("-" * 50)

        external = [t for t in top_n if "External" in t["api_type"]]
        internal = [t for t in top_n if "Internal" in t["api_type"]]
        ext_total = sum(t["duration_ms"] for t in external)
        int_total = sum(t["duration_ms"] for t in internal)
        lines.append(f"  🌐 External: {len(external)} calls, {ext_total:,}ms total")
        lines.append(f"  🏠 Internal: {len(internal)} calls, {int_total:,}ms total")
        lines.append("=" * 50)
        return "\n".join(lines)

    def get_status_summary(self):
        if not self.is_enabled:
            return "OFF"
        avg_max = (sum(self.rolling_latencies) / len(self.rolling_latencies)) if self.rolling_latencies else 0
        return (
            f"ON | polls={self.poll_count}, slow_calls={self.total_slow_calls}, "
            f"avg_max_latency={avg_max:.0f}ms"
        )


def run_latency_monitor(running_flag):
    """API Latency monitor main loop (runs in its own thread)."""
    monitor = LatencyMonitor()
    if not monitor.is_enabled:
        latency_log("API Latency monitor is disabled.")
        return

    cfg = LATENCY_MONITOR_CONFIG
    latency_log("=" * 60)
    latency_log("API Latency Monitor Started")
    latency_log(f"  Namespace:       {cfg['namespace']}")
    latency_log(f"  App:             {cfg['app_label']}")
    latency_log(f"  Interval:        {cfg['poll_interval_seconds']}s")
    latency_log(f"  Lookback:        {cfg['lookback_minutes']}min")
    latency_log(f"  Slow threshold:  {cfg['slow_threshold_ms']}ms (single call)")
    latency_log(f"  Total threshold: {cfg['total_threshold_ms']}ms (full request)")
    latency_log(f"  Top N:           {cfg['top_n']}")
    latency_log(f"  Cooldown:        {cfg['cooldown_seconds']}s")
    latency_log("=" * 60)

    # Test connection
    latency_log("Testing Loki/Grafana connection...")
    ok, msg, method = monitor.client.test_connection()
    if ok:
        latency_log(f"OK {msg}")
    else:
        latency_log(f"FAIL {msg}", "ERROR")
        return

    # Initial poll
    try:
        timings, top_n, alerts = monitor.poll()
        latency_log(f"Initial scan: {len(timings)} timing entries, top latency: "
                     f"{top_n[0]['duration_ms']:,}ms" if top_n else "Initial scan: no timing data")
        if top_n:
            for i, t in enumerate(top_n[:3], 1):
                latency_log(f"  #{i} {t['duration_ms']:,}ms — {t['api_type']} {t['api_name'][:60]}")
        for alert_payload in alerts:
            send_status_slack(alert_payload)
    except Exception as ex:
        latency_log(f"Initial poll failed: {ex}", "ERROR")

    # Main loop
    while running_flag["active"]:
        for _ in range(cfg["poll_interval_seconds"] * 10):
            if not running_flag["active"]:
                break
            time.sleep(0.1)
        if not running_flag["active"]:
            break

        try:
            timings, top_n, alerts = monitor.poll()
            if timings:
                max_lat = top_n[0]["duration_ms"] if top_n else 0
                latency_log(
                    f"Poll #{monitor.poll_count}: {len(timings)} entries, "
                    f"max={max_lat:,}ms, slow={len([t for t in timings if t['duration_ms'] >= cfg['slow_threshold_ms']])}"
                )
            else:
                latency_log(f"Poll #{monitor.poll_count}: no timing data")

            for alert_payload in alerts:
                ok = send_status_slack(alert_payload)
                if ok:
                    latency_log("Slack latency alert sent!", "ALERT")
                else:
                    latency_log("Slack latency alert FAILED!", "ERROR")

        except Exception as ex:
            latency_log(f"Poll failed: {ex}", "ERROR")

    latency_log("API Latency monitor stopped.")




# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 2A — STATUS MONITOR HELPERS                   ║
# ╚════════════════════════════════════════════════════════════════════════════╝

STATUS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

HEALTHY_STATUSES = {"operational", "online", "up", "ok", "healthy", "active"}
DEGRADED_STATUSES = {"degraded", "degraded performance", "partial outage", "partial degradation", "partial", "delayed"}
DOWN_STATUSES = {"major outage", "down", "outage", "severe degradation", "unavailable", "offline"}


def status_http_get(url, retries=MAX_RETRIES, accept_json=False):
    if not HAS_REQUESTS:
        raise ImportError("requests library required for status monitor. Run: pip install requests")
    headers = {**STATUS_HEADERS}
    if accept_json:
        headers["Accept"] = "application/json"
    for attempt in range(1, retries + 1):
        try:
            resp = req_lib.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except req_lib.RequestException as e:
            status_log(f"  Attempt {attempt}/{retries} for {url}: {e}", "WARN")
            if attempt < retries:
                time.sleep(3 * attempt)
            else:
                raise


def classify_status(status):
    s = status.strip().lower()
    if s in HEALTHY_STATUSES: return "healthy"
    elif s in DEGRADED_STATUSES: return "degraded"
    elif s in DOWN_STATUSES: return "down"
    elif "outage" in s or "down" in s or "severe" in s: return "down"
    elif "degrad" in s or "partial" in s or "delay" in s: return "degraded"
    else: return "unknown"


def is_healthy(status):
    return classify_status(status) == "healthy"



# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 2C — CONNECTX CHECKER                         ║
# ╚════════════════════════════════════════════════════════════════════════════╝

def check_connectx(config):
    services = {}
    parsed = urlparse(config["status_url"])
    base = f"{parsed.scheme}://{parsed.netloc}"

    for path in config.get("api_endpoints", []):
        try:
            url = base + path
            resp = req_lib.get(url, headers={"Accept": "application/json"}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                components = data.get("components", data.get("result", []))
                if isinstance(components, list):
                    for comp in components:
                        name = comp.get("name", "Unknown")
                        status = comp.get("status", "unknown").replace("_", " ").title()
                        group = comp.get("group", comp.get("group_name", ""))
                        key = f"ConnectX > {group} > {name}" if group else f"ConnectX > {name}"
                        services[key] = status
                if services:
                    status_log(f"  ConnectX: Fetched via API ({url})")
                    return services
        except Exception:
            continue

    if not HAS_BS4:
        raise ImportError("beautifulsoup4 required for HTML scraping. Run: pip install beautifulsoup4")

    resp = status_http_get(config["status_url"])
    soup = BeautifulSoup(resp.text, "html.parser")

    # ── SIMPLE APPROACH: Find service names paired with uptime percentages ──
    # The ConnectX status page shows each service with "XX.XXX% uptime".
    # We ONLY use the uptime percentage to determine status.
    # PATCH v6.3: Uptime % is HISTORICAL (e.g. 30-day rolling), not real-time.
    # A brief outage earlier today can drop uptime to 98.x% for the rest of
    # the day even though the service is currently healthy.  Using 99% as the
    # "Operational" threshold caused false "Degraded" alerts for services that
    # had already recovered.  Thresholds lowered: >= 95% = Operational.
    UPTIME_OPERATIONAL_THRESHOLD = 95.0   # was 99.0 — too sensitive for historical %
    UPTIME_DEGRADED_THRESHOLD   = 80.0   # was 90.0
    all_elements = soup.find_all(True)
    for elem in all_elements:
        text = elem.get_text(strip=True)
        if not text or len(text) > 200:
            continue
        # Look for elements containing uptime percentage
        uptime_m = re.search(r'([\d.]+)%\s*uptime', text, re.IGNORECASE)
        if not uptime_m:
            continue
        # Get service name from previous sibling
        prev = elem.find_previous_sibling()
        if not prev:
            continue
        name = prev.get_text(strip=True)
        if not name or len(name) > 60:
            continue
        # Determine status ONLY from uptime percentage
        try:
            uptime_pct = float(uptime_m.group(1))
        except (ValueError, IndexError):
            continue
        if uptime_pct >= UPTIME_OPERATIONAL_THRESHOLD:
            services[f"ConnectX > {name}"] = "Operational"
        elif uptime_pct >= UPTIME_DEGRADED_THRESHOLD:
            services[f"ConnectX > {name}"] = "Degraded"
        else:
            services[f"ConnectX > {name}"] = "Down"

    # NOTE: aria-label scraping removed — it was picking up historical
    # "Degraded" labels from uptime bar charts and overwriting correct
    # Operational status. The uptime percentage check above is reliable.

    if not services:
        # Fallback: search page text for known service names and their uptime
        page_text = soup.get_text()
        # Find ALL uptime values on the page
        all_uptimes = re.findall(r'([A-Za-z][\w\s-]{2,40}?)\s+([\d.]+)%\s*uptime', page_text)
        for name, uptime_str in all_uptimes:
            name = name.strip()
            try:
                uptime_pct = float(uptime_str)
            except ValueError:
                continue
            if uptime_pct >= UPTIME_OPERATIONAL_THRESHOLD:
                services[f"ConnectX > {name}"] = "Operational"
            elif uptime_pct >= UPTIME_DEGRADED_THRESHOLD:
                services[f"ConnectX > {name}"] = "Degraded"
            else:
                services[f"ConnectX > {name}"] = "Down"

    if not services:
        page_text = soup.get_text().lower()
        if "all services are online" in page_text or "all systems operational" in page_text:
            services["ConnectX > Overall"] = "Operational"
        else:
            services["ConnectX > Overall"] = "Unknown"

    # ── Post-processing: override false "Degraded" using actual uptime % ──
    # PATCH v6.3: Use same threshold as main scraping logic
    page_full_text = soup.get_text()
    for svc_key in list(services.keys()):
        if services[svc_key] in ("Degraded", "Degraded Performance", "Unknown"):
            # Extract the service name (after "ConnectX > ")
            svc_name = svc_key.replace("ConnectX > ", "")
            # Search the page text for this service's uptime
            pattern = re.escape(svc_name) + r'[\s\S]{0,100}?([\d.]+)%\s*uptime'
            uptime_match = re.search(pattern, page_full_text, re.IGNORECASE)
            if uptime_match:
                try:
                    uptime_pct = float(uptime_match.group(1))
                    if uptime_pct >= UPTIME_OPERATIONAL_THRESHOLD:
                        services[svc_key] = "Operational"
                except (ValueError, IndexError):
                    pass

    return services


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║            SECTION 2D — STATUS MONITOR CORE (PATCHED: FIRST-RUN)         ║
# ╚════════════════════════════════════════════════════════════════════════════╝

STATUS_CHECKERS = {
    "connectx": check_connectx,
}


def check_all_services():
    all_services = {}
    errors = {}
    for svc_id, config in SERVICES.items():
        if not config.get("enabled", True): continue
        checker = STATUS_CHECKERS.get(svc_id)
        if not checker:
            status_log(f"No checker for service: {svc_id}", "WARN"); continue
        try:
            status_log(f"  Checking {config['name']}...")
            result = checker(config)
            all_services.update(result)
            status_log(f"  {config['icon']} {config['name']}: {len(result)} components found")
        except Exception as e:
            status_log(f"  {config['name']} check failed: {e}", "ERROR")
            errors[svc_id] = str(e)
    return all_services, errors


def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            status_log("Corrupted state file, starting fresh.", "WARN")
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def send_status_slack(payload):
    """Send Slack alert with retry logic (PATCHED v6.1)."""
    if not HAS_REQUESTS:
        status_log("requests library not available for Slack", "ERROR"); return False
    if SLACK_CHANNEL:
        payload["channel"] = SLACK_CHANNEL

    for attempt in range(1, 4):  # PATCH v6.1: 3 retries
        try:
            resp = req_lib.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
            if resp.status_code == 200:
                status_log("  Slack alert sent.")
                return True
            else:
                status_log(f"  Slack attempt {attempt}/3: HTTP {resp.status_code} - {resp.text[:100]}", "ERROR")
        except req_lib.RequestException as e:
            status_log(f"  Slack attempt {attempt}/3 failed: {e}", "ERROR")
        if attempt < 3:
            time.sleep(2 * attempt)

    status_log("  !!! SLACK STATUS ALERT DELIVERY FAILED after 3 retries!", "ERROR")
    return False


def build_status_alert(alert_type, changed_services, all_services=None):
    if alert_type == "down":
        color = "#FF0000"; pretext = "\U0001f6a8 *Service Alert \u2014 Service(s) DOWN*"; emoji = "\U0001f534"
    elif alert_type == "recovered":
        color = "#36A64F"; pretext = "\u2705 *Service Alert \u2014 Service(s) RECOVERED*"; emoji = "\U0001f7e2"
    else:
        color = "#FFA500"; pretext = "\u26a0\ufe0f *Service Alert \u2014 Monitor Error*"; emoji = "\u26a0\ufe0f"

    grouped = {}
    for name, status in changed_services.items():
        parts = name.split(" > ", 1)
        platform = parts[0] if len(parts) > 1 else "Unknown"
        component = parts[1] if len(parts) > 1 else name
        if platform not in grouped: grouped[platform] = []
        grouped[platform].append((component, status))

    fields = []
    for platform, components in grouped.items():
        svc_id = None
        for sid, cfg in SERVICES.items():
            if cfg["name"] == platform: svc_id = sid; break
        icon = SERVICES[svc_id]["icon"] if svc_id else "\U0001f539"
        link = SERVICES[svc_id]["status_page_link"] if svc_id else ""
        for comp, status in components:
            fields.append({
                "title": f"{emoji} {icon} {platform} \u2014 {comp}",
                "value": f"Status: *{status}*" + (f"\n<{link}|View Status Page>" if link else ""),
                "short": True,
            })

    if all_services and alert_type == "down":
        text = f"*{len(changed_services)}* service(s) are experiencing issues."
    elif alert_type == "recovered":
        text = f"*{len(changed_services)}* service(s) have recovered."
    else:
        text = "\n".join(f"\u2022 {k}: {v}" for k, v in changed_services.items())

    return {
        "attachments": [{
            "color": color, "pretext": pretext, "text": text, "fields": fields,
            "footer": "Unified Monitor v6.1 | ConnectX",
            "footer_icon": "https://img.icons8.com/color/48/000000/monitor.png",
            "ts": int(time.time()),
        }],
    }


def build_error_alert(errors):
    text = "\n".join(
        f"\u2022 *{SERVICES[k]['name']}*: {v}" for k, v in errors.items() if k in SERVICES
    )
    return {
        "attachments": [{
            "color": "#FFA500",
            "pretext": "\u26a0\ufe0f *Status Monitor \u2014 Cannot Reach Status Page(s)*",
            "text": f"Failed to check the following services:\n{text}",
            "footer": "Unified Monitor v6.1 | ConnectX",
            "ts": int(time.time()),
        }],
    }


def run_status_check():
    """Perform a single status check cycle across all services. (PATCHED v6.3 - DEBOUNCE + COOLDOWN)"""
    status_log("=" * 55)
    status_log(f"Status check at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    prev_state = load_state()
    prev_services = prev_state.get("services", {})
    consecutive_errors = prev_state.get("consecutive_errors", {})
    # PATCH v6.2: Track how many consecutive polls each service has been unhealthy/healthy
    unhealthy_streak = prev_state.get("unhealthy_streak", {})
    healthy_streak = prev_state.get("healthy_streak", {})
    # Track which services we've already alerted as DOWN (to avoid re-alerting)
    alerted_down = prev_state.get("alerted_down", {})
    # PATCH v6.3: Track when we last alerted per service (cooldown to prevent rapid re-alerts)
    last_alert_time = prev_state.get("last_alert_time", {})
    is_first_run = len(prev_services) == 0

    DEBOUNCE_DOWN_COUNT = 3     # PATCH v6.3: was 2 — require 3 consecutive unhealthy checks (15 min)
    DEBOUNCE_RECOVER_COUNT = 3  # PATCH v6.3: was same as down — require 3 consecutive healthy checks
    ALERT_COOLDOWN_SECS = 1800  # PATCH v6.3: Don't re-alert same service within 30 minutes

    current_services, errors = check_all_services()

    if current_services:
        status_log(f"  {'SERVICE':<45} {'STATUS':<25} {'HEALTH'}")
        status_log(f"  {'\u2500' * 45} {'\u2500' * 25} {'\u2500' * 8}")
        for name, status in sorted(current_services.items()):
            health = classify_status(status)
            icon = "\u2705" if health == "healthy" else "\u26a0\ufe0f" if health == "degraded" else "\u274c" if health == "down" else "\u2753"
            status_log(f"  {icon} {name:<43} {status:<25} {health}")

    for svc_id, err in errors.items():
        consecutive_errors[svc_id] = consecutive_errors.get(svc_id, 0) + 1
        if consecutive_errors[svc_id] == 3:
            send_status_slack(build_error_alert({svc_id: err}))

    for svc_id in SERVICES:
        if svc_id not in errors:
            consecutive_errors[svc_id] = 0

    # ── PATCH v6.3: DEBOUNCED transition detection with COOLDOWN ──
    # Only alert DOWN after service is unhealthy for DEBOUNCE_DOWN_COUNT consecutive checks.
    # Only alert RECOVERED after service is healthy for DEBOUNCE_RECOVER_COUNT consecutive checks.
    # Don't re-alert the same service within ALERT_COOLDOWN_SECS even if it clears and re-triggers.

    for name, status in current_services.items():
        now_healthy = is_healthy(status)
        if now_healthy:
            healthy_streak[name] = healthy_streak.get(name, 0) + 1
            unhealthy_streak[name] = 0
        else:
            unhealthy_streak[name] = unhealthy_streak.get(name, 0) + 1
            healthy_streak[name] = 0

    newly_down = {}
    newly_recovered = {}
    now_ts = int(time.time())

    for name, status in current_services.items():
        curr_class = classify_status(status)
        was_alerted = alerted_down.get(name, False)

        # Alert DOWN: service unhealthy for DEBOUNCE_DOWN_COUNT consecutive checks
        #             AND not already alerted AND cooldown has elapsed
        if not was_alerted and unhealthy_streak.get(name, 0) >= DEBOUNCE_DOWN_COUNT:
            svc_last_alert = last_alert_time.get(name, 0)
            # last_alert_time=0 means never alerted → always allow
            if svc_last_alert == 0 or (now_ts - svc_last_alert) >= ALERT_COOLDOWN_SECS:
                newly_down[name] = status
                alerted_down[name] = True
                last_alert_time[name] = now_ts
                status_log(f"  CONFIRMED unhealthy ({unhealthy_streak[name]} consecutive checks): {name}", "WARN")
            else:
                remaining = ALERT_COOLDOWN_SECS - (now_ts - svc_last_alert)
                status_log(f"  {name} unhealthy but in cooldown ({remaining}s remaining), suppressing alert", "WARN")

        # Alert RECOVERED: was alerted AND now healthy for DEBOUNCE_RECOVER_COUNT consecutive checks
        elif was_alerted and healthy_streak.get(name, 0) >= DEBOUNCE_RECOVER_COUNT:
            newly_recovered[name] = status
            alerted_down[name] = False
            status_log(f"  CONFIRMED recovered ({healthy_streak[name]} consecutive checks): {name}")

    # First run: if a service is already unhealthy, mark it immediately (no debounce needed)
    if is_first_run or FORCE_ALERT:
        for name, status in current_services.items():
            if not is_healthy(status) and name not in newly_down:
                newly_down[name] = status
                alerted_down[name] = True
                last_alert_time[name] = now_ts
                unhealthy_streak[name] = DEBOUNCE_DOWN_COUNT

    if newly_down:
        status_log(f"\U0001f6a8 {len(newly_down)} service(s) impacted: {list(newly_down.keys())}", "WARN")
        send_status_slack(build_status_alert("down", newly_down, current_services))

    if newly_recovered:
        status_log(f"\u2705 {len(newly_recovered)} service(s) recovered: {list(newly_recovered.keys())}")
        send_status_slack(build_status_alert("recovered", newly_recovered))

    if not newly_down and not newly_recovered:
        currently_unhealthy = {k: v for k, v in current_services.items() if not is_healthy(v)}
        if currently_unhealthy:
            pending = {k: v for k, v in currently_unhealthy.items()
                       if unhealthy_streak.get(k, 0) < DEBOUNCE_DOWN_COUNT}
            confirmed = {k: v for k, v in currently_unhealthy.items()
                         if alerted_down.get(k, False)}
            if pending:
                status_log(f"\u26a0\ufe0f  {len(pending)} service(s) unhealthy (pending confirmation, {DEBOUNCE_DOWN_COUNT - unhealthy_streak.get(list(pending.keys())[0], 0)} more check(s) needed)")
            if confirmed:
                status_log(f"\u26a0\ufe0f  {len(confirmed)} service(s) still impacted (already alerted)")
            if not pending and not confirmed:
                status_log(f"\u26a0\ufe0f  {len(currently_unhealthy)} service(s) still impacted (no change)")
        else:
            status_log("\u2705 All services operational across all platforms.")

    save_state({
        "services": current_services,
        "consecutive_errors": consecutive_errors,
        "unhealthy_streak": unhealthy_streak,
        "healthy_streak": healthy_streak,
        "alerted_down": alerted_down,
        "last_alert_time": last_alert_time,
        "last_check": datetime.now(timezone.utc).isoformat(),
        "last_errors": errors if errors else None,
    })


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 3 — UNIFIED MAIN                              ║
# ╚════════════════════════════════════════════════════════════════════════════╝

def print_order_status(engine):
    o = engine.orders
    total = len(o)
    drafts = sum(1 for r in o.values() if r.state == "draft")
    inprog = sum(1 for r in o.values() if r.is_in_progress)
    alerted = sum(1 for r in o.values() if r.alert_fired)
    paid = sum(1 for r in o.values() if r.became_inprogress_at and r.has_payment_info)
    refunded = sum(1 for r in o.values() if r.refund_completed_at)
    done = sum(1 for r in o.values() if r.milestone_at or r.esim_at or r.state == "completed")
    alerts = len(engine.alerts)
    order_log(f"Orders: {total} total | {drafts} draft | {inprog} inProgress | "
        f"{alerted} alerted | {paid} paid | {refunded} refunded | {done} completed | {alerts} alerts")


def run_order_monitor(running_flag):
    """Order monitor main loop (runs in its own thread)."""
    order_log("=" * 60)
    order_log("Order Auto-Monitor v5.5 \u2014 Instant Payment Alert + Pre-Move Validation")
    order_log("=" * 60)
    order_log(f"Grafana:    {ORDER_CONFIG['grafana_url']}")
    order_log(f"Namespace:  {ORDER_CONFIG['namespace']}")
    order_log(f"Alert:      Instant on inProgress")
    order_log(f"Poll:       {ORDER_CONFIG['poll_interval_seconds']}s")
    order_log(f"Slack:      {'ON' if ORDER_SLACK_CONFIG.get('enabled') and ORDER_SLACK_CONFIG.get('webhook_url') else 'OFF'}")
    order_log(f"Email:      {'ON -> ' + EMAIL_CONFIG['to'] if EMAIL_CONFIG.get('smtp_password') else 'OFF (no SMTP_PASSWORD)'}")
    order_log(f"Force:      {'ON' if FORCE_ALERT else 'OFF'}")

    engine = AutoMonitorEngine()

    db_status = "ON" if engine.db_client.is_enabled else "OFF (set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE)"
    order_log(f"Databricks: {db_status}")
    order_log("-" * 60)

    order_log("Testing Grafana connection...")
    ok, msg, method = engine.client.test_connection()
    if ok:
        order_log(f"OK {msg}")
    else:
        order_log(f"FAIL {msg}", "ERROR")
        order_log("Cannot connect to Grafana. Check GRAFANA_USER and GRAFANA_PASSWORD.", "ERROR")
        return

    order_log(f"Initial scan (last {ORDER_CONFIG['initial_lookback_minutes']}min)...")
    try:
        events, alerts = engine.poll(is_initial=True)
        order_log(f"OK Found {len(engine.orders)} orders, {len(events)} lifecycle events")
        for ts, oid, etype, detail in events:
            order_log(f"  #{oid} {etype} [{short_ist(ts)}]")
        for ts, oid, alert_msg in alerts:
            order_log(f"ALERT: {alert_msg}", "ALERT")
            ok_s, r_s = OrderSlackSender.send(oid, alert_msg)
            order_log(f"  Slack: {r_s}")
            ok_e, r_e = EmailAlertSender.send(oid, alert_msg)
            order_log(f"  Email: {r_e}")
            # PATCH v6.1: Summary of delivery status
            if not ok_s and not ok_e:
                order_log(f"  !!! ALERT FOR ORDER #{oid} COULD NOT BE DELIVERED VIA ANY CHANNEL!", "ERROR")
    except Exception as ex:
        order_log(f"FAIL Initial scan failed: {ex}", "ERROR")

    print_order_status(engine)
    order_log("-" * 60)
    order_log("Order monitoring started.")
    order_log("-" * 60)

    while running_flag["active"]:
        for _ in range(ORDER_CONFIG["poll_interval_seconds"] * 10):
            if not running_flag["active"]: break
            time.sleep(0.1)
        if not running_flag["active"]: break

        try:
            events, alerts = engine.poll()
            if events:
                order_log(f"Poll #{engine.poll_count}: {len(events)} events")
                for ts, oid, etype, detail in events:
                    order_log(f"  #{oid} {etype} [{short_ist(ts)}]")
            else:
                order_log(f"Poll #{engine.poll_count}: no new events")

            for ts, oid, alert_msg in alerts:
                order_log(f"PAYMENT ALERT!", "ALERT")
                order_log(alert_msg, "ALERT")
                ok_s, r_s = OrderSlackSender.send(oid, alert_msg)
                order_log(f"  Slack: {r_s}")
                ok_e, r_e = EmailAlertSender.send(oid, alert_msg)
                order_log(f"  Email: {r_e}")
                # PATCH v6.1: loud failure if both channels fail
                if not ok_s and not ok_e:
                    order_log(f"  !!! ALERT FOR ORDER #{oid} COULD NOT BE DELIVERED VIA ANY CHANNEL!", "ERROR")

            if engine.poll_count % 10 == 0:
                print_order_status(engine)

        except Exception as ex:
            order_log(f"Poll failed: {ex}", "ERROR")

    order_log("Order monitor stopped.")
    print_order_status(engine)


def run_status_monitor(running_flag):
    """Status monitor main loop (runs in its own thread)."""
    global FORCE_ALERT
    status_log("=" * 55)
    status_log("Multi-Service Status Monitor Started")
    status_log(f"  Services: {', '.join(c['name'] for c in SERVICES.values() if c.get('enabled'))}")
    status_log(f"  Interval: {STATUS_CHECK_INTERVAL}s ({STATUS_CHECK_INTERVAL // 60} min)")
    status_log(f"  State:    {STATE_FILE}")
    status_log(f"  Slack:    Configured")
    status_log(f"  Force:    {'ON' if FORCE_ALERT else 'OFF'}")
    status_log("=" * 55)

    run_status_check()

    # PATCH v6.1: Reset force flag after first run (only force once)
    if FORCE_ALERT:
        status_log("--force-alert: First check complete, clearing force flag for subsequent checks.")
        FORCE_ALERT = False

    while running_flag["active"]:
        status_log(f"Next status check in {STATUS_CHECK_INTERVAL}s...")
        for _ in range(STATUS_CHECK_INTERVAL * 10):
            if not running_flag["active"]: break
            time.sleep(0.1)
        if not running_flag["active"]: break
        try:
            run_status_check()
        except Exception as e:
            status_log(f"Status check error: {e}", "ERROR")
            time.sleep(60)

    status_log("Status monitor stopped.")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 4 — CLI ENTRY POINT                           ║
# ╚════════════════════════════════════════════════════════════════════════════╝

def main():
    parser = argparse.ArgumentParser(
        description="Unified Order & Service Monitor v6.5 (Order + ConnectX + Latency) [PATCHED]")
    parser.add_argument("--order-only", action="store_true", help="Run only the order monitor")
    parser.add_argument("--status-only", action="store_true", help="Run only the status monitor")
    parser.add_argument("--error-pattern-only", action="store_true", help="Run only the Loki error pattern monitor")
    parser.add_argument("--latency-only", action="store_true", help="Run only the API latency monitor")
    parser.add_argument("--no-error-pattern", action="store_true", help="Disable Loki error pattern monitor")
    parser.add_argument("--no-latency", action="store_true", help="Disable API latency monitor")
    parser.add_argument("--status-once", action="store_true", help="Single status check and exit")
    parser.add_argument("--test-slack", action="store_true", help="Send test Slack message and exit")
    parser.add_argument("--force-alert", action="store_true",
                        help="Force re-trigger alerts for all currently impacted services/orders (PATCH v6.1)")
    parser.add_argument("--reset-state", action="store_true",
                        help="Delete state file to force fresh baseline (PATCH v6.1)")
    parser.add_argument("--webhook", type=str, help="Override Slack webhook URL")
    parser.add_argument("--disable", type=str, nargs="+",
                        help="Disable status services (e.g., --disable stripe)")
    parser.add_argument("--only-status", type=str, nargs="+",
                        help="Only check specific status services (e.g., --only-status stripe)")
    args = parser.parse_args()

    global SLACK_WEBHOOK_URL, FORCE_ALERT
    if args.webhook:
        SLACK_WEBHOOK_URL = args.webhook
        ORDER_SLACK_CONFIG["webhook_url"] = args.webhook

    if args.force_alert:
        FORCE_ALERT = True

    # PATCH v6.1: --reset-state deletes state file for clean baseline
    if args.reset_state:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
            print(f"[INFO] Deleted state file: {STATE_FILE}")
        else:
            print(f"[INFO] No state file to delete: {STATE_FILE}")
        if not (args.status_once or args.status_only or args.order_only or args.test_slack):
            print("[INFO] Use with --status-once or other flags to run after reset.")
            sys.exit(0)

    if args.disable:
        for svc in args.disable:
            if svc in SERVICES:
                SERVICES[svc]["enabled"] = False
                print(f"[INFO] Disabled: {svc}")

    if args.only_status:
        for svc_id in SERVICES:
            SERVICES[svc_id]["enabled"] = svc_id in args.only_status

    # --- Test Slack ---
    if args.test_slack:
        print("[INFO] Sending test Slack message...")
        if HAS_REQUESTS:
            test_payload = {
                "attachments": [{
                    "color": "#36A64F",
                    "pretext": "\U0001f9ea *Unified Monitor v6.1 \u2014 Test Alert*",
                    "text": "Monitoring: Orders (Loki/Grafana) + " + ", ".join(
                        f"{c['icon']} {c['name']}" for c in SERVICES.values() if c.get("enabled")
                    ),
                    "fields": [
                        {"title": "\U0001f4e6 Order Monitor", "value": "Connection OK", "short": True},
                        {"title": "\u2705 ConnectX", "value": "Connection OK", "short": True},
                        {"title": "\U0001f50d Error Pattern", "value": f"Watching {LOKI_ERROR_PATTERN_CONFIG['namespace']}", "short": True},
                        {"title": "⏱️ API Latency", "value": f"Top {LATENCY_MONITOR_CONFIG['top_n']} | >{LATENCY_MONITOR_CONFIG['slow_threshold_ms']}ms", "short": True},
                    ],
                    "footer": "Unified Monitor v6.5 | Orders + ConnectX + Errors + Latency",
                    "ts": int(time.time()),
                }],
            }
            success = send_status_slack(test_payload)
            if success:
                print("[OK] Slack test message sent successfully!")
            else:
                print("[FAIL] Slack test message FAILED! Check webhook URL.", file=sys.stderr)
            sys.exit(0 if success else 1)
        else:
            print("[ERROR] 'requests' library required. pip install requests")
            sys.exit(1)

    # --- Single status check ---
    if args.status_once:
        run_status_check()
        sys.exit(0)

    # --- Banner ---
    ts = fmt_ist(now_utc())
    print(f"\n{'=' * 65}")
    print(f"  UNIFIED ORDER & SERVICE MONITOR v6.5 (PATCHED)")
    print(f"  Started: {ts}")
    print(f"{'=' * 65}")

    mode = "BOTH"
    if args.order_only: mode = "ORDER ONLY"
    if args.status_only: mode = "STATUS ONLY"
    if args.error_pattern_only: mode = "ERROR PATTERN ONLY"
    if args.latency_only: mode = "LATENCY ONLY"
    print(f"  Mode:          {mode}")
    print(f"  Order Poll:    {ORDER_CONFIG['poll_interval_seconds']}s")
    print(f"  Status Poll:   {STATUS_CHECK_INTERVAL}s ({STATUS_CHECK_INTERVAL // 60} min)")
    ep_cfg = LOKI_ERROR_PATTERN_CONFIG
    ep_status = "OFF" if args.no_error_pattern else f"{ep_cfg['poll_interval_seconds']}s | {ep_cfg['namespace']} / {ep_cfg['app_label']}"
    print(f"  Error Pattern: {ep_status}")
    lat_cfg = LATENCY_MONITOR_CONFIG
    lat_status = "OFF" if args.no_latency else f"{lat_cfg['poll_interval_seconds']}s | threshold: {lat_cfg['slow_threshold_ms']}ms | top {lat_cfg['top_n']}"
    print(f"  API Latency:   {lat_status}")
    print(f"  Slack:         {'Configured' if SLACK_WEBHOOK_URL else 'NOT configured'}")
    print(f"  Force Alert:   {'YES' if FORCE_ALERT else 'No'}")
    print(f"  Services:      {', '.join(c['name'] for c in SERVICES.values() if c.get('enabled'))}")
    print(f"{'=' * 65}\n")

    # --- Run ---
    running_flag = {"active": True}

    def shutdown(sig, frame):
        print(f"\n[{fmt_ist(now_utc())}] [MAIN] Shutting down gracefully...")
        running_flag["active"] = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    threads = []

    if not args.status_only and not args.error_pattern_only and not args.latency_only:
        t_order = threading.Thread(target=run_order_monitor, args=(running_flag,),
                                   name="OrderMonitor", daemon=True)
        threads.append(t_order)

    if not args.order_only and not args.error_pattern_only and not args.latency_only:
        if HAS_REQUESTS:
            t_status = threading.Thread(target=run_status_monitor, args=(running_flag,),
                                        name="StatusMonitor", daemon=True)
            threads.append(t_status)
        else:
            print("[WARN] 'requests' library not installed \u2014 Status monitor disabled.")
            print("[WARN] Install with: pip install requests beautifulsoup4")

    # Loki Error Pattern Monitor thread
    if not args.no_error_pattern and (
        not args.order_only and not args.status_only and not args.latency_only or args.error_pattern_only
    ):
        if LOKI_ERROR_PATTERN_CONFIG.get("enabled", False):
            t_error = threading.Thread(target=run_error_pattern_monitor, args=(running_flag,),
                                       name="ErrorPatternMonitor", daemon=True)
            threads.append(t_error)
        else:
            print("[INFO] Loki error pattern monitor disabled in config.")

    # API Latency Monitor thread
    if not args.no_latency and (
        not args.order_only and not args.status_only and not args.error_pattern_only or args.latency_only
    ):
        if LATENCY_MONITOR_CONFIG.get("enabled", False):
            t_latency = threading.Thread(target=run_latency_monitor, args=(running_flag,),
                                          name="LatencyMonitor", daemon=True)
            threads.append(t_latency)
        else:
            print("[INFO] API Latency monitor disabled in config.")

    for t in threads:
        t.start()

    # Keep main thread alive for signal handling
    while running_flag["active"]:
        time.sleep(0.5)

    # Wait for threads to finish
    for t in threads:
        t.join(timeout=5)

    print(f"[{fmt_ist(now_utc())}] [MAIN] All monitors stopped. Goodbye!")


if __name__ == "__main__":
    main()

