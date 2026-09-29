"""
Exception rules -- the pipeline's own logic; everything else is exported from dbt.

queries() defines the Athena datasets the rules read. Each rule is a pure
function over those rows and returns exception records for the review report:

    {rule, severity, reference, amount, detail}
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from .athena import FINANCE, SEEDS, between, timestamp_literal

OUTLIER_IQR_MULTIPLIER = 3

# Duplicate detection compares each order with the ones just before it, so the
# orders query reaches back this far before the period start.
DUPLICATE_WINDOW_MINUTES = 10


def queries(start: date, end: date) -> dict[str, str]:
    """Athena SQL for every dataset the rules read. Period rows are [start, end)."""
    dup_start = f"{timestamp_literal(start)} - interval '{DUPLICATE_WINDOW_MINUTES}' minute"
    return {
        "revenue": f"""
            select order_id, shipped_at, recognized_revenue, cutoff_status
            from {FINANCE}.revenue
            where {between("shipped_at", start, end)} and recognized_revenue <> 0
        """,
        "shipped_items": f"""
            select i.order_item_id, i.order_id, i.product_category, i.sale_price, i.shipped_at,
                   coalesce(t.risk_tier, 'Unrated') as risk_tier
            from {FINANCE}.order_item_revenue as i
            left join {SEEDS}.audit_materiality_thresholds as t
                on i.product_category = t.product_category
            where {between("i.shipped_at", start, end)}
        """,
        "reconciliation": f"""
            select order_id, order_at, total_order_amount, order_reconciliation,
                   order_item_count_recon, order_status_recon_detail
            from {FINANCE}.order_reconciliation
            where {between("order_at", start, end)}
        """,
        "orders": f"""
            select order_id, user_id, gross_revenue, created_at
            from {FINANCE}.revenue
            where created_at >= {dup_start} and created_at < {timestamp_literal(end)}
        """,
        "refunds": f"""
            select order_id, order_date, gross_revenue, refund_amount
            from {FINANCE}.refund_reconciliation
            where {between("order_date", start, end)} and refund_amount > 0
        """,
        # Whole-history datasets: outlier baselines and future-dated shipments.
        "category_stats": f"""
            select product_category,
                   approx_percentile(sale_price, 0.25) as q1,
                   approx_percentile(sale_price, 0.75) as q3,
                   count(*) as n
            from {FINANCE}.order_item_revenue
            group by product_category
        """,
        "future_dated": f"""
            select order_id, shipped_at, recognized_revenue
            from {FINANCE}.revenue
            where shipped_at > current_timestamp
        """,
    }


EXCEPTION_COLUMNS = ("rule", "severity", "reference", "amount", "detail")


def _exception(rule: str, severity: str, reference: str, amount, detail: str) -> dict:
    return dict(zip(EXCEPTION_COLUMNS, (rule, severity, reference, amount, detail)))


def reconciliation_exceptions(rows: list[dict]) -> list[dict]:
    """Master/sub-ledger breaks. Explained variances (MIXED FULFILLMENT ...) are not exceptions."""
    out = []
    for r in rows:
        problems = []
        if r["order_reconciliation"] != "RECONCILIATION SUCCESSFUL":
            problems.append(r["order_reconciliation"])
        if r["order_item_count_recon"] == "VARIANCE DETECTED":
            problems.append("item count variance")
        if r["order_status_recon_detail"] == "VARIANCE DETECTED - UNEXPLAINED":
            problems.append("unexplained status variance")
        if problems:
            out.append(
                _exception("RECONCILIATION_BREAK", "High", r["order_id"], r["total_order_amount"], "; ".join(problems))
            )
    return out


def cutoff_risks(rows: list[dict]) -> list[dict]:
    return [
        _exception(
            "CUTOFF_RISK",
            "Medium",
            r["order_id"],
            r["recognized_revenue"],
            f"Ordered and shipped in different months (shipped {r['shipped_at']:%Y-%m-%d})",
        )
        for r in rows
        if r["cutoff_status"] == "POTENTIAL CUT-OFF RISK"
    ]


def future_dated(rows: list[dict], as_of: datetime) -> list[dict]:
    return [
        _exception(
            "FUTURE_DATED_SHIPMENT",
            "High",
            r["order_id"],
            r["recognized_revenue"],
            f"shipped_at {r['shipped_at']:%Y-%m-%d %H:%M} is after run time {as_of:%Y-%m-%d %H:%M} UTC",
        )
        for r in rows
        if r["shipped_at"] > as_of
    ]


def duplicate_orders(rows: list[dict], period_start: datetime) -> list[dict]:
    """Same customer and amount within the duplicate window. Only orders created in the period are flagged."""
    window = timedelta(minutes=DUPLICATE_WINDOW_MINUTES)
    ordered = sorted(rows, key=lambda r: (r["user_id"], r["gross_revenue"], r["created_at"]))
    out = []
    for prev, cur in zip(ordered, ordered[1:]):
        if (
            cur["created_at"] >= period_start
            and cur["user_id"] == prev["user_id"]
            and cur["gross_revenue"] == prev["gross_revenue"]
            and cur["created_at"] - prev["created_at"] <= window
        ):
            out.append(
                _exception(
                    "DUPLICATE_SUSPECT",
                    "High",
                    cur["order_id"],
                    cur["gross_revenue"],
                    f"Same customer and amount as order {prev['order_id']} "
                    f"{(cur['created_at'] - prev['created_at']).seconds // 60} min earlier",
                )
            )
    return out


def amount_outliers(rows: list[dict], category_stats: list[dict]) -> list[dict]:
    """Item price above Q3 + k*IQR for its category; severity follows the CPA-defined risk tier seed."""
    fences = {s["product_category"]: s["q3"] + OUTLIER_IQR_MULTIPLIER * (s["q3"] - s["q1"]) for s in category_stats}
    out = []
    for r in rows:
        fence = fences.get(r["product_category"])
        if fence is not None and r["sale_price"] > fence:
            out.append(
                _exception(
                    "AMOUNT_OUTLIER",
                    r["risk_tier"],
                    r["order_item_id"],
                    r["sale_price"],
                    f"{r['product_category']}: {r['sale_price']:.2f} exceeds Q3+{OUTLIER_IQR_MULTIPLIER}xIQR "
                    f"fence {fence:.2f}",
                )
            )
    return out


def refund_exceeds_revenue(rows: list[dict]) -> list[dict]:
    return [
        _exception(
            "REFUND_EXCEEDS_REVENUE",
            "High",
            r["order_id"],
            r["refund_amount"],
            f"Refund {r['refund_amount']:.2f} > gross revenue {r['gross_revenue']:.2f}",
        )
        for r in rows
        if r["refund_amount"] > r["gross_revenue"]
    ]


def run_all(data: dict[str, list[dict]], period_start: datetime, as_of: datetime) -> list[dict]:
    return (
        reconciliation_exceptions(data["reconciliation"])
        + cutoff_risks(data["revenue"])
        + future_dated(data["future_dated"], as_of)
        + duplicate_orders(data["orders"], period_start)
        + amount_outliers(data["shipped_items"], data["category_stats"])
        + refund_exceeds_revenue(data["refunds"])
    )
