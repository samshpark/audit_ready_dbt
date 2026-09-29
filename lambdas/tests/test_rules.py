from datetime import datetime

from je_pipeline import rules

PERIOD_START = datetime(2026, 8, 1)


def _recon(order_id, recon="RECONCILIATION SUCCESSFUL", count="INTEGRITY VERIFIED", detail="INTEGRITY VERIFIED"):
    return {
        "order_id": order_id,
        "total_order_amount": 100.0,
        "order_reconciliation": recon,
        "order_item_count_recon": count,
        "order_status_recon_detail": detail,
    }


def test_reconciliation_flags_breaks_but_not_explained_variances():
    rows = [
        _recon("ok"),
        _recon("explained", detail="MIXED FULFILLMENT - PARTIAL REFUND"),
        _recon("orphan", recon="ERR: ORPHAN SUB-LEDGER", count="N/A - PREVIOUS ERROR"),
        _recon("count", count="VARIANCE DETECTED"),
        _recon("status", detail="VARIANCE DETECTED - UNEXPLAINED"),
    ]
    flagged = {e["reference"] for e in rules.reconciliation_exceptions(rows)}
    assert flagged == {"orphan", "count", "status"}


def test_cutoff_risk():
    rows = [
        {"order_id": "a", "shipped_at": datetime(2026, 8, 1), "recognized_revenue": 10.0, "cutoff_status": "NORMAL"},
        {
            "order_id": "b",
            "shipped_at": datetime(2026, 8, 1),
            "recognized_revenue": 10.0,
            "cutoff_status": "POTENTIAL CUT-OFF RISK",
        },
    ]
    assert [e["reference"] for e in rules.cutoff_risks(rows)] == ["b"]


def test_future_dated_shipment():
    as_of = datetime(2026, 9, 29, 12)
    rows = [
        {"order_id": "past", "shipped_at": datetime(2026, 9, 29, 11), "recognized_revenue": 1.0},
        {"order_id": "future", "shipped_at": datetime(2026, 9, 30, 8), "recognized_revenue": 1.0},
    ]
    assert [e["reference"] for e in rules.future_dated(rows, as_of)] == ["future"]


def _order(order_id, user, amount, created):
    return {"order_id": order_id, "user_id": user, "gross_revenue": amount, "created_at": created}


def test_duplicate_orders_within_window_only():
    rows = [
        _order("o1", "u1", 50.0, datetime(2026, 8, 1, 10, 0)),
        _order("o2", "u1", 50.0, datetime(2026, 8, 1, 10, 7)),  # 7 min later -> duplicate
        _order("o3", "u1", 50.0, datetime(2026, 8, 1, 11, 0)),  # 53 min later -> not
        _order("o4", "u2", 50.0, datetime(2026, 8, 1, 10, 1)),  # different customer
        _order("o5", "u1", 51.0, datetime(2026, 8, 1, 10, 2)),  # different amount
    ]
    flagged = rules.duplicate_orders(rows, PERIOD_START)
    assert [e["reference"] for e in flagged] == ["o2"]
    assert "o1" in flagged[0]["detail"]


def test_duplicate_before_period_is_context_not_flagged():
    # The earlier order sits in the look-back window before the period; only
    # the in-period order is flagged, and a pair entirely before is ignored.
    rows = [
        _order("prev", "u1", 50.0, datetime(2026, 7, 31, 23, 55)),
        _order("cur", "u1", 50.0, datetime(2026, 8, 1, 0, 2)),
        _order("old1", "u2", 20.0, datetime(2026, 7, 31, 23, 51)),
        _order("old2", "u2", 20.0, datetime(2026, 7, 31, 23, 52)),
    ]
    assert [e["reference"] for e in rules.duplicate_orders(rows, PERIOD_START)] == ["cur"]


def test_amount_outlier_uses_category_iqr_and_risk_tier():
    stats = [{"product_category": "suits", "q1": 100.0, "q3": 200.0, "n": 50}]  # fence = 200 + 3*100 = 500
    rows = [
        {"order_item_id": "i1", "product_category": "suits", "sale_price": 499.0, "risk_tier": "High"},
        {"order_item_id": "i2", "product_category": "suits", "sale_price": 750.0, "risk_tier": "High"},
        {"order_item_id": "i3", "product_category": "unknown", "sale_price": 9999.0, "risk_tier": "Unrated"},
    ]
    flagged = rules.amount_outliers(rows, stats)
    assert [(e["reference"], e["severity"]) for e in flagged] == [("i2", "High")]


def test_refund_exceeds_revenue():
    rows = [
        {"order_id": "ok", "gross_revenue": 100.0, "refund_amount": 100.0},
        {"order_id": "bad", "gross_revenue": 100.0, "refund_amount": 120.0},
    ]
    assert [e["reference"] for e in rules.refund_exceeds_revenue(rows)] == ["bad"]
