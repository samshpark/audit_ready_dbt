from datetime import date
from decimal import Decimal

import pytest

from je_pipeline import handler


def test_period_defaults_to_yesterday():
    assert handler.resolve_period({}, date(2026, 9, 29)) == (date(2026, 9, 28), date(2026, 9, 29))


def test_period_end_date_is_inclusive():
    event = {"start_date": "2026-08-01", "end_date": "2026-08-31"}
    assert handler.resolve_period(event, date(2026, 9, 29)) == (date(2026, 8, 1), date(2026, 9, 1))


def test_period_rejects_reversed_dates():
    with pytest.raises(ValueError):
        handler.resolve_period({"start_date": "2026-08-31", "end_date": "2026-08-01"}, date(2026, 9, 29))


def test_amounts_render_with_two_decimals():
    rows = [{"je_id": "JE-1", "debit_amount": 893.2, "credit_amount": 0.0}]
    assert handler.format_money(rows, ("debit_amount", "credit_amount")) == [
        {"je_id": "JE-1", "debit_amount": "893.20", "credit_amount": "0.00"}
    ]


def test_net_by_account_is_debit_positive_and_exact():
    # 0.1 + 0.2 != 0.3 in float; summed as Decimal it is exact.
    entries = [
        {"account_code": 1200, "account_name": "Accounts Receivable", "debit_amount": 0.1, "credit_amount": 0.0},
        {"account_code": 1200, "account_name": "Accounts Receivable", "debit_amount": 0.2, "credit_amount": 0.0},
        {"account_code": 4000, "account_name": "Sales Revenue", "debit_amount": 0.0, "credit_amount": 0.3},
    ]
    assert handler.net_by_account(entries) == {
        "1200 Accounts Receivable": Decimal("0.30"),
        "4000 Sales Revenue": Decimal("-0.30"),
    }


def test_empty_period_still_writes_csv_header():
    # A day with no postings must be distinguishable from a truncated file.
    assert handler._csv([], handler.JE_COLUMNS).splitlines() == [",".join(handler.JE_COLUMNS)]
