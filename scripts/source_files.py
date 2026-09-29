"""
Shared definition of the dbt source layer's Parquet files, used by the
warehouse loaders (load_to_s3_athena.py, load_to_bigquery.py) so every
warehouse is loaded from the same files with the same type handling.
"""

import io

import pyarrow.parquet as pq

# dbt source name -> tables; each table is data/<table>.parquet
SOURCES = {
    "thelook_ecommerce": [
        "raw_orders",
        "raw_order_items",
        "raw_products",
        "raw_users",
        "raw_inventory_items",
    ],
    "incremental": [
        "incr_orders",
        "incr_order_items",
        "incr_inventory_items",
    ],
}


def to_parquet_bytes(path: str) -> tuple:
    """Re-encode a Parquet file with microsecond timestamps.

    incr_*.parquet are written by pandas with nanosecond timestamps, which
    Athena's Parquet reader rejects; microseconds load cleanly everywhere.
    """
    table = pq.read_table(path)
    buf = io.BytesIO()
    pq.write_table(table, buf, coerce_timestamps="us", allow_truncated_timestamps=True)
    return buf.getvalue(), table.schema
