"""
Daily incremental data generator for the Airflow pipeline.

Writes synthetic orders to three parquet files, simulating a full
purchase-and-sale cycle so the dbt incremental models and inventory ledger
both stay consistent:

  incr_inventory_items  ← inbound: item received into warehouse (created_at before order)
  incr_order_items      ← outbound: item sold / shipped (links back via inventory_item_id)
  incr_orders           ← order header matching the line items above

Negative IDs are used for all synthetic records to avoid collision with the
real BigQuery-sourced data.

Two steps per run:

  1. Create the business date's orders (default: yesterday UTC, or the Airflow
     run's `ds`). A date that already has orders is skipped, so re-runs and
     Airflow catch-up never duplicate a day.
  2. Advance every synthetic order's lifecycle to the current time. Each order
     has a fixed plan (refund type, shipping delay, transit and return times,
     backlog outcome) derived from its order_id, and only events whose planned
     time has already passed are written — so no timestamp or status is ever
     recorded ahead of the event. Orders move through Processing → Shipped →
     Complete (→ Returned) on later runs as their planned times arrive.

Creation is seeded by date and lifecycle plans by order_id, so the dataset is
reproducible: `--reset --backfill-from 2025-06-01` rebuilds the same orders on
any machine (only lifecycle progress depends on when it is run).

Usage:
    python scripts/generate_daily_incremental.py                   # yesterday
    python scripts/generate_daily_incremental.py --date 2026-09-28
    python scripts/generate_daily_incremental.py --reset --backfill-from 2025-06-01
"""

import argparse
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd

# The BigQuery source is a one-time, immutable snapshot that stops at this
# date (~15 orders/day at that point). Past it, every order comes from this
# generator, so its daily count picks up the same organic growth rate the
# BigQuery history was already showing instead of flatlining forever.
_ANCHOR_DATE = date(2026, 6, 21)
_ANCHOR_DAILY_ORDERS = 15
_MONTHLY_GROWTH_RATE = 0.05

ORDER_TYPES = ("FULLY REFUNDED", "PARTIALLY REFUNDED", "NO REFUND")
ORDER_TYPE_WEIGHTS = (0.07, 0.15, 0.78)

# A share of NO REFUND orders sit in an order backlog before shipping, matching
# the ~30-40% pending rate in the BigQuery-sourced data. Without this every
# synthetic order ships within 48h and recognized_revenue trends toward 100%
# for any recent window dominated by incremental rows.
BACKLOG_RATE = 0.30
# Each backlog day from day 2 on: ships with 65%, is cancelled with 5%,
# otherwise keeps aging (a genuine cut-off risk signal).
BACKLOG_DAILY_SHIP, BACKLOG_DAILY_CANCEL = 0.65, 0.05

ORDER_COLUMNS = [
    "order_id", "user_id", "status", "gender", "created_at",
    "returned_at", "shipped_at", "delivered_at", "num_of_item",
]  # fmt: skip
ITEM_COLUMNS = [
    "id", "order_id", "user_id", "product_id", "inventory_item_id", "status",
    "created_at", "shipped_at", "delivered_at", "returned_at", "sale_price",
]  # fmt: skip
INV_COLUMNS = [
    "id", "product_id", "created_at", "sold_at", "cost", "product_category",
    "product_name", "product_brand", "product_retail_price", "product_department",
    "product_sku", "product_distribution_center_id",
]  # fmt: skip


@dataclass(frozen=True)
class LifecyclePlan:
    """An order's fate, fixed at creation. Delays are relative to the previous event."""

    order_type: str
    ship_after: Optional[timedelta]  # None: cancelled before shipping
    cancel_after: Optional[timedelta]
    deliver_after: timedelta
    return_after: timedelta

    def returned_item_count(self, n_items: int) -> int:
        if self.order_type == "FULLY REFUNDED":
            return n_items
        return 1 if self.order_type == "PARTIALLY REFUNDED" else 0


def lifecycle_plan(order_id: int) -> LifecyclePlan:
    rng = random.Random(f"lifecycle:{order_id}")
    order_type = rng.choices(ORDER_TYPES, ORDER_TYPE_WEIGHTS)[0]
    ship_after = timedelta(hours=rng.randint(24, 48))
    cancel_after = None
    if order_type == "NO REFUND" and rng.random() < BACKLOG_RATE:
        days, ship_after = 2, None
        while ship_after is None and cancel_after is None:
            roll = rng.random()
            if roll < BACKLOG_DAILY_SHIP:
                ship_after = timedelta(days=days, hours=rng.randint(0, 20))
            elif roll < BACKLOG_DAILY_SHIP + BACKLOG_DAILY_CANCEL:
                cancel_after = timedelta(days=days)
            days += 1
    return LifecyclePlan(
        order_type=order_type,
        ship_after=ship_after,
        cancel_after=cancel_after,
        deliver_after=timedelta(hours=rng.randint(24, 72)),
        return_after=timedelta(hours=rng.randint(24, 168)),
    )


def _growth_adjusted_order_count(business_date: date) -> int:
    months_elapsed = max(0.0, (business_date - _ANCHOR_DATE).days / 30)
    return max(1, round(_ANCHOR_DAILY_ORDERS * (1 + _MONTHLY_GROWTH_RATE) ** months_elapsed))


def _next_id(df: pd.DataFrame, column: str) -> int:
    return int(df[column].min()) - 1 if not df.empty and df[column].notna().any() else -1


def create_orders(
    business_date: date,
    now: datetime,
    ids: dict,
    users: list,
    user_gender: dict,
    products: dict,
) -> tuple:
    """Static fields of the orders placed on business_date; lifecycle fields are set by advance_lifecycles().

    `ids` holds the next negative order / item / inventory id and is advanced in place.
    """
    rng = random.Random(f"orders:{business_date.isoformat()}")
    day_start = datetime(business_date.year, business_date.month, business_date.day, tzinfo=timezone.utc)
    latest_minute = min(1439, int((now - day_start).total_seconds() // 60))
    product_ids = sorted(products)

    orders, items, inv = [], [], []
    for _ in range(_growth_adjusted_order_count(business_date)):
        order_id = ids["order"]
        ids["order"] -= 1
        plan = lifecycle_plan(order_id)
        user_id = rng.choice(users)
        created_at = day_start + timedelta(minutes=rng.randint(0, latest_minute))
        n_items = rng.randint(1, 3)
        if plan.order_type == "PARTIALLY REFUNDED":
            n_items = max(n_items, 2)

        for _ in range(n_items):
            product_id = rng.choice(product_ids)
            prod = products[product_id]
            # Use '' for missing string attrs to match BigQuery-sourced inventory_items,
            # where unknown values are stored as empty strings, not NULLs.
            inv.append(
                {
                    "id": ids["inv"],
                    "product_id": product_id,
                    "created_at": created_at - timedelta(days=rng.randint(7, 60)),
                    "cost": prod.get("cost"),
                    "product_category": prod.get("category") or "",
                    "product_name": prod.get("name") or "",
                    "product_brand": prod.get("brand") or "",
                    "product_retail_price": prod.get("retail_price"),
                    "product_department": prod.get("department") or "",
                    "product_sku": prod.get("sku") or "",
                    "product_distribution_center_id": prod.get("distribution_center_id"),
                }
            )
            items.append(
                {
                    "id": ids["item"],
                    "order_id": order_id,
                    "user_id": user_id,
                    "product_id": product_id,
                    "inventory_item_id": ids["inv"],
                    "created_at": created_at,
                    "sale_price": float(rng.randint(20, 200)),
                }
            )
            ids["item"] -= 1
            ids["inv"] -= 1

        orders.append(
            {
                "order_id": order_id,
                "user_id": user_id,
                "gender": user_gender.get(user_id, "M"),
                "created_at": created_at,
                "num_of_item": n_items,
            }
        )
    return orders, items, inv


def order_state(order_id: int, created_at: datetime, n_items: int, now: datetime) -> dict:
    """Order header and per-item lifecycle as of `now`: only events that have already happened."""
    plan = lifecycle_plan(order_id)
    if plan.cancel_after is not None and created_at + plan.cancel_after <= now:
        return {"status": "Cancelled", "shipped_at": None, "delivered_at": None, "returned_at": None,
                "items": [("Cancelled", None)] * n_items}  # fmt: skip

    ship_at = created_at + plan.ship_after if plan.ship_after is not None else None
    shipped = ship_at if ship_at is not None and ship_at <= now else None
    deliver_at = shipped + plan.deliver_after if shipped else None
    delivered = deliver_at if deliver_at is not None and deliver_at <= now else None
    return_at = delivered + plan.return_after if delivered else None
    returned = return_at if return_at is not None and return_at <= now else None

    if shipped is None:
        item_status = "Processing"
    elif delivered is None:
        item_status = "Shipped"
    else:
        item_status = "Complete"
    n_returned = plan.returned_item_count(n_items) if returned else 0
    items = [("Returned", returned)] * n_returned + [(item_status, None)] * (n_items - n_returned)

    if n_returned == n_items:
        status = "Returned"
    else:
        status = item_status  # a partial refund leaves the order Complete
    return {"status": status, "shipped_at": shipped, "delivered_at": delivered,
            "returned_at": returned if n_returned else None, "items": items}  # fmt: skip


# Columns derived from each order's lifecycle plan. They are recomputed from
# scratch on every run, so only the static columns are carried between runs.
LIFECYCLE_COLUMNS = ("status", "shipped_at", "delivered_at", "returned_at")


def advance_lifecycles(orders: pd.DataFrame, items: pd.DataFrame, inv: pd.DataFrame, now: datetime) -> tuple:
    """Return (orders, items, inv) with every lifecycle field set as of `now`."""
    # Items within an order are numbered in creation order: ids count down, so
    # the first item created has the highest id and is the one a partial refund returns.
    orders = orders.sort_values("order_id", ascending=False, ignore_index=True)
    items = items.sort_values(["order_id", "id"], ascending=[False, False], ignore_index=True)
    item_counts = items.groupby("order_id").size()

    states = [
        order_state(int(order_id), created_at.to_pydatetime(), int(item_counts[order_id]), now)
        for order_id, created_at in zip(orders["order_id"], orders["created_at"])
    ]
    for col in LIFECYCLE_COLUMNS:
        orders[col] = [state[col] for state in states]

    by_order = dict(zip(orders["order_id"], states))
    item_states = [s for state in states for s in state["items"]]  # same order as `items`
    items["status"] = [status for status, _ in item_states]
    items["returned_at"] = [returned_at for _, returned_at in item_states]
    for col in ("shipped_at", "delivered_at"):
        items[col] = [by_order[order_id][col] for order_id in items["order_id"]]

    # A unit leaves inventory for good once it is delivered and kept.
    kept = items[items["status"] == "Complete"]
    inv = inv.copy()
    inv["sold_at"] = inv["id"].map(dict(zip(kept["inventory_item_id"], kept["shipped_at"])))

    ts = ("shipped_at", "delivered_at", "returned_at")
    return (
        _cast(orders, ORDER_COLUMNS, ts, ()),
        _cast(items.sort_values("id", ascending=False, ignore_index=True), ITEM_COLUMNS, ts, ()),
        _cast(inv, INV_COLUMNS, ("sold_at",), ()),
    )


def _cast(df: pd.DataFrame, columns: list, ts_cols: tuple, int_cols: tuple) -> pd.DataFrame:
    df = df.reindex(columns=columns)
    for col in ts_cols:
        df[col] = pd.to_datetime(df[col], utc=True).astype("datetime64[ns, UTC]")
    for col in int_cols:
        df[col] = df[col].astype("Int64")
    return df


def _append(existing: pd.DataFrame, new_rows: list, columns: list, ts_cols: tuple, int_cols: tuple) -> pd.DataFrame:
    """Append new rows to the loaded frame; only static (non-lifecycle) columns are kept."""
    static = [c for c in columns if c not in LIFECYCLE_COLUMNS and c != "sold_at"]
    frames = [
        _cast(f, static, ts_cols, int_cols)
        for f in (existing, pd.DataFrame(new_rows, columns=static))
        if not f.empty
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=static)


def generate(
    project_dir: str = ".",
    business_dates: Optional[list] = None,
    now: Optional[datetime] = None,
    reset: bool = False,
) -> None:
    now = now or datetime.now(timezone.utc)
    business_dates = business_dates or [now.date() - timedelta(days=1)]
    if max(business_dates) > now.date():
        raise ValueError(f"Cannot create orders for a future date: {max(business_dates)}")
    data_dir = os.path.join(project_dir, "data")
    paths = {n: os.path.join(data_dir, f"incr_{n}.parquet") for n in ("orders", "order_items", "inventory_items")}

    def load(name: str) -> pd.DataFrame:
        return pd.read_parquet(paths[name]) if os.path.exists(paths[name]) and not reset else pd.DataFrame()

    orders, items, inv = load("orders"), load("order_items"), load("inventory_items")
    users_df = pd.read_parquet(os.path.join(data_dir, "raw_users.parquet"))
    products_df = pd.read_parquet(os.path.join(data_dir, "raw_products.parquet"))
    users = sorted(users_df["id"].unique().tolist())
    user_gender = users_df.set_index("id")["gender"].to_dict()
    products = products_df.set_index("id")[
        ["cost", "category", "name", "brand", "retail_price", "department", "sku", "distribution_center_id"]
    ].to_dict("index")

    ids = {"order": _next_id(orders, "order_id"), "item": _next_id(items, "id"), "inv": _next_id(inv, "id")}
    existing_dates = set(pd.to_datetime(orders["created_at"], utc=True).dt.date) if not orders.empty else set()

    new_orders, new_items, new_inv, created, skipped = [], [], [], [], []
    for d in sorted(business_dates):
        if d in existing_dates:
            skipped.append(d)
            continue
        o, i, v = create_orders(d, now, ids, users, user_gender, products)
        new_orders += o
        new_items += i
        new_inv += v
        created.append(d)

    ts = ("created_at",)
    orders = _append(orders, new_orders, ORDER_COLUMNS, ts, ("order_id", "user_id", "num_of_item"))
    item_ids = ("id", "order_id", "user_id", "product_id", "inventory_item_id")
    items = _append(items, new_items, ITEM_COLUMNS, ts, item_ids)
    inv = _append(inv, new_inv, INV_COLUMNS, ts, ("id", "product_id", "product_distribution_center_id"))

    orders, items, inv = advance_lifecycles(orders, items, inv, now)

    os.makedirs(data_dir, exist_ok=True)
    orders.to_parquet(paths["orders"], index=False)
    items.to_parquet(paths["order_items"], index=False)
    inv.to_parquet(paths["inventory_items"], index=False)

    if created:
        span = f"{created[0]}..{created[-1]}" if len(created) > 1 else str(created[0])
        print(f"[SUCCESS] created orders for {len(created)} day(s): {span}")
    if skipped:
        print(f"[SKIP] already generated: {', '.join(map(str, skipped[:5]))}{' ...' if len(skipped) > 5 else ''}")
    print(f"  lifecycles advanced to {now:%Y-%m-%d %H:%M} UTC")
    print(f"  incr_orders         : {len(orders):,} rows")
    print(f"  incr_order_items    : {len(items):,} rows")
    print(f"  incr_inventory_items: {len(inv):,} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("project_dir", nargs="?", default=".")
    parser.add_argument("--date", type=date.fromisoformat, help="Business date to create (default: yesterday UTC)")
    parser.add_argument("--backfill-from", type=date.fromisoformat, help="Create every date from here through --date")
    parser.add_argument("--reset", action="store_true", help="Discard existing incr_*.parquet and start fresh")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    end = args.date or now.date() - timedelta(days=1)
    start = args.backfill_from or end
    dates = [start + timedelta(days=n) for n in range((end - start).days + 1)]
    generate(args.project_dir, dates, now, reset=args.reset)


if __name__ == "__main__":
    main()
