import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

import generate_daily_incremental as g

REPO_DATA = Path(__file__).resolve().parents[2] / "data"
NOW = datetime(2026, 9, 29, 14, 47, tzinfo=timezone.utc)
DATES = [date(2026, 7, 1) + timedelta(days=n) for n in range(90)]  # through 2026-09-28


def _project(tmp_path: Path) -> str:
    (tmp_path / "data").mkdir(parents=True)
    for name in ("raw_users", "raw_products"):
        shutil.copy(REPO_DATA / f"{name}.parquet", tmp_path / "data")
    return str(tmp_path)


def _read(project: str) -> tuple:
    names = ("orders", "order_items", "inventory_items")
    return tuple(pd.read_parquet(f"{project}/data/incr_{n}.parquet") for n in names)


@pytest.fixture
def generated(tmp_path):
    project = _project(tmp_path)
    g.generate(project, DATES, NOW, reset=True)
    return project


def test_no_event_is_recorded_before_it_happens(generated):
    # The defect this generator was rewritten for: shipments, deliveries, and
    # returns were written ahead of time, so revenue was recognized pre-shipment.
    orders, items, inv = _read(generated)
    now = pd.Timestamp(NOW)
    for df in (orders, items):
        for col in ("created_at", "shipped_at", "delivered_at", "returned_at"):
            assert not (df[col] > now).any(), col
    assert not (inv["sold_at"] > now).any()


def test_events_follow_lifecycle_order(generated):
    orders, items, _ = _read(generated)
    for df in (orders, items):
        assert not (df["shipped_at"] < df["created_at"]).any()
        assert not (df["delivered_at"] < df["shipped_at"]).any()
        assert not (df["returned_at"] < df["delivered_at"]).any()


def test_status_matches_recorded_events(generated):
    _, items, inv = _read(generated)
    assert items.loc[items.status == "Processing", "shipped_at"].isna().all()
    assert items.loc[items.status == "Shipped", "delivered_at"].isna().all()
    assert items.loc[items.status == "Complete", "delivered_at"].notna().all()
    assert items.loc[items.status == "Returned", "returned_at"].notna().all()
    sold = items.merge(inv[["id", "sold_at"]], left_on="inventory_item_id", right_on="id", suffixes=("", "_inv"))
    assert sold.loc[sold.status == "Complete", "sold_at"].notna().all()
    assert sold.loc[sold.status != "Complete", "sold_at"].isna().all()


def test_header_agrees_with_lines(generated):
    orders, items, _ = _read(generated)
    lines = items.groupby("order_id").agg(n=("id", "size"), returned=("status", lambda s: (s == "Returned").sum()))
    joined = orders.set_index("order_id").join(lines)
    assert (joined.num_of_item == joined.n).all()
    assert (joined.loc[joined.status == "Returned", "returned"] == joined.loc[joined.status == "Returned", "n"]).all()
    partial = joined[(joined.returned > 0) & (joined.returned < joined.n)]
    assert len(partial) > 0 and (partial.status == "Complete").all()


def test_rerunning_a_generated_day_changes_nothing(generated):
    before = _read(generated)
    g.generate(generated, [DATES[-1]], NOW)
    assert all(a.equals(b) for a, b in zip(before, _read(generated)))


def test_backfill_is_reproducible(tmp_path):
    a, b = _project(tmp_path / "a"), _project(tmp_path / "b")
    g.generate(a, DATES, NOW, reset=True)
    g.generate(b, DATES, NOW, reset=True)
    assert all(x.equals(y) for x, y in zip(_read(a), _read(b)))


def test_later_run_advances_orders_without_rewriting_history(generated):
    before, *_ = _read(generated)
    later = NOW + timedelta(days=3)
    g.generate(generated, [date(2026, 9, 29)], later)
    after, *_ = _read(generated)

    old = before.set_index("order_id")
    new = after.set_index("order_id").loc[old.index]
    assert (old.status != new.status).any()  # pending orders progressed
    shipped = old.shipped_at.notna()
    assert (old.shipped_at[shipped] == new.shipped_at[shipped]).all()
    assert not (after[["shipped_at", "delivered_at", "returned_at"]] > pd.Timestamp(later)).any().any()


def test_future_business_date_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        g.generate(_project(tmp_path), [NOW.date() + timedelta(days=1)], NOW)


def test_lifecycle_plan_is_fixed_per_order():
    assert g.lifecycle_plan(-123) == g.lifecycle_plan(-123)
