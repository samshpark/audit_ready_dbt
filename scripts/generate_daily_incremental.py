"""
Daily incremental data generator for the Airflow pipeline.

Appends synthetic orders for today to three parquet files, simulating a full
purchase-and-sale cycle so the dbt incremental models and inventory ledger
both stay consistent:

  raw_inventory_items  ← inbound: item received into warehouse (created_at before order)
  raw_order_items      ← outbound: item sold / shipped (links back via inventory_item_id)
  raw_orders           ← order header matching the line items above

Negative IDs are used for all synthetic records to avoid collision with the
real BigQuery-sourced data.
"""

import os
import sys
import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


def generate_today_orders(project_dir: str = ".", n_orders: int = 100) -> None:
    data_dir = os.path.join(project_dir, "data")
    items_path    = os.path.join(data_dir, "incr_order_items.parquet")
    orders_path   = os.path.join(data_dir, "incr_orders.parquet")
    users_path    = os.path.join(data_dir, "raw_users.parquet")
    inv_path      = os.path.join(data_dir, "incr_inventory_items.parquet")
    products_path = os.path.join(data_dir, "raw_products.parquet")

    existing_items  = pd.read_parquet(items_path)
    existing_orders = pd.read_parquet(orders_path)
    existing_inv    = pd.read_parquet(inv_path)
    user_df         = pd.read_parquet(users_path)
    product_df      = pd.read_parquet(products_path)

    real_user_ids   = user_df["id"].unique().tolist()
    real_product_ids = existing_items["product_id"].drop_duplicates().tolist()
    user_gender_map  = user_df.set_index("id")["gender"].to_dict()

    # Product lookup: product_id → attributes needed for raw_inventory_items
    product_map = product_df.set_index("id")[
        ["cost", "category", "name", "brand", "retail_price", "department", "sku", "distribution_center_id"]
    ].to_dict("index")

    # Continue negative-ID sequences so synthetic rows never collide with real data
    neg_mask = existing_items["id"] < 0
    next_item_id = int(existing_items.loc[neg_mask, "id"].min()) - 1 if neg_mask.any() else -1

    neg_mask = existing_orders["order_id"] < 0
    next_order_id = int(existing_orders.loc[neg_mask, "order_id"].min()) - 1 if neg_mask.any() else -1

    neg_mask = existing_inv["id"] < 0
    next_inv_item_id = int(existing_inv.loc[neg_mask, "id"].min()) - 1 if neg_mask.any() else -1

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    new_items_rows = []
    new_orders_rows = []
    new_inv_rows = []

    for i in range(n_orders):
        order_id         = next_order_id - i
        user_id          = random.choice(real_user_ids)
        order_created_at = today + timedelta(minutes=random.randint(0, 540))

        order_type = np.random.choice(
            ["FULLY REFUNDED", "PARTIALLY REFUNDED", "NO REFUND"],
            p=[0.07, 0.15, 0.78],
        )
        items_in_order = random.randint(1, 3)
        if order_type == "PARTIALLY REFUNDED":
            items_in_order = max(items_in_order, 2)
        shipped_at     = order_created_at + timedelta(hours=random.randint(24, 48))
        delivered_at   = shipped_at + timedelta(hours=random.randint(24, 72))

        order_returned_at = None
        order_status = "Returned" if order_type == "FULLY REFUNDED" else "Complete"

        for j in range(items_in_order):
            item_id          = next_item_id
            next_item_id    -= 1
            inventory_item_id = next_inv_item_id
            next_inv_item_id -= 1
            product_id       = random.choice(real_product_ids)
            prod             = product_map.get(product_id, {})

            returned_at = None
            if order_type == "FULLY REFUNDED":
                status      = "Returned"
                returned_at = delivered_at + timedelta(hours=random.randint(24, 168))
                order_returned_at = returned_at
            elif order_type == "PARTIALLY REFUNDED":
                status = "Returned" if j == 0 else "Complete"
                if status == "Returned":
                    returned_at = delivered_at + timedelta(hours=random.randint(24, 168))
                    order_returned_at = returned_at
            else:
                status = np.random.choice(["Complete", "Shipped"], p=[0.9, 0.1])
                if status == "Shipped":
                    order_status = "Shipped"

            # Inbound: item received into warehouse some days before the order
            inv_created_at = order_created_at - timedelta(days=random.randint(7, 60))
            inv_sold_at    = shipped_at if status not in ("Returned", "Shipped") else None

            # Use '' for missing string attrs to match BigQuery-sourced inventory_items
            # where unknown values are stored as empty strings, not NULLs.
            new_inv_rows.append({
                "id":                           inventory_item_id,
                "product_id":                   product_id,
                "created_at":                   inv_created_at,
                "sold_at":                      inv_sold_at,
                "cost":                         prod.get("cost"),
                "product_category":             prod.get("category") or "",
                "product_name":                 prod.get("name") or "",
                "product_brand":                prod.get("brand") or "",
                "product_retail_price":         prod.get("retail_price"),
                "product_department":           prod.get("department") or "",
                "product_sku":                  prod.get("sku") or "",
                "product_distribution_center_id": prod.get("distribution_center_id"),
            })

            new_items_rows.append({
                "id":                 item_id,
                "order_id":           order_id,
                "user_id":            user_id,
                "product_id":         product_id,
                "inventory_item_id":  inventory_item_id,
                "status":             status,
                "created_at":         order_created_at,
                "shipped_at":         shipped_at,
                "delivered_at":       delivered_at if status != "Shipped" else None,
                "returned_at":        returned_at,
                "sale_price":         float(random.randint(20, 200)),
            })

        new_orders_rows.append({
            "order_id":   order_id,
            "user_id":    user_id,
            "status":     order_status,
            "gender":     user_gender_map.get(user_id, "M"),
            "created_at": order_created_at,
            "returned_at": order_returned_at,
            "shipped_at": shipped_at,
            "delivered_at": delivered_at,
            "num_of_item": items_in_order,
        })

    new_items_df  = pd.DataFrame(new_items_rows)
    new_orders_df = pd.DataFrame(new_orders_rows)
    new_inv_df    = pd.DataFrame(new_inv_rows)

    # Cast timestamps to UTC
    for df in [new_items_df, new_orders_df]:
        for col in ["created_at", "shipped_at", "delivered_at", "returned_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
    for col in ["created_at", "sold_at"]:
        new_inv_df[col] = pd.to_datetime(new_inv_df[col], utc=True)

    # Cast integer IDs to nullable Int64
    for col in ["id", "order_id", "user_id", "product_id", "inventory_item_id"]:
        if col in new_items_df.columns:
            new_items_df[col] = new_items_df[col].astype("Int64")
    for col in ["order_id", "user_id", "num_of_item"]:
        if col in new_orders_df.columns:
            new_orders_df[col] = new_orders_df[col].astype("Int64")
    for col in ["id", "product_id", "product_distribution_center_id"]:
        if col in new_inv_df.columns:
            new_inv_df[col] = new_inv_df[col].astype("Int64")

    updated_items  = pd.concat([existing_items, new_items_df],  ignore_index=True)
    updated_orders = pd.concat([existing_orders, new_orders_df], ignore_index=True)
    updated_inv    = pd.concat([existing_inv, new_inv_df],       ignore_index=True)

    updated_items.to_parquet(items_path,  index=False)
    updated_orders.to_parquet(orders_path, index=False)
    updated_inv.to_parquet(inv_path,      index=False)

    print(f"[SUCCESS] {today.date()}: appended {len(new_items_df)} items across {n_orders} orders")
    print(f"  raw_order_items    : {len(existing_items):,} → {len(updated_items):,} rows")
    print(f"  raw_orders         : {len(existing_orders):,} → {len(updated_orders):,} rows")
    print(f"  raw_inventory_items: {len(existing_inv):,} → {len(updated_inv):,} rows")


if __name__ == "__main__":
    project_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    generate_today_orders(project_dir)
