"""
Tableau Public Export Script

Exports dbt mart tables from DuckDB to CSV files for use in Tableau Public.
Tableau Public does not support live database connections, so this script
produces static snapshots that can be loaded directly into Tableau.

Output: tableau_exports/{table_name}.csv

Usage:
    python scripts/export_for_tableau.py [--db-path path/to/dev.duckdb]
"""

import argparse
import os
import sys

import duckdb

# schema.table — order_item_revenue lives in main_finance (custom dbt schema),
# the remaining four are materialized in the default main schema.
MART_TABLES = [
    ("main_finance", "revenue"),
    ("main_finance", "order_reconciliation"),
    ("main_finance", "refund_reconciliation"),
    ("main_finance", "inventory_fiscal_report"),
    ("main_finance", "order_item_revenue"),
    ("main_finance", "inventory_sellthrough"),
]


def export_tables(db_path: str) -> None:
    if not os.path.exists(db_path):
        print(f"[ERROR] Database not found: {db_path}")
        sys.exit(1)

    output_dir = os.path.join(os.path.dirname(os.path.abspath(db_path)), "tableau_exports")
    os.makedirs(output_dir, exist_ok=True)

    con = duckdb.connect(db_path, read_only=True)

    print()
    print("=" * 55)
    print("  Tableau Export")
    print(f"  Source : {db_path}")
    print(f"  Output : {output_dir}/")
    print("=" * 55)

    for schema, table in MART_TABLES:
        df = con.execute(f"SELECT * FROM {schema}.{table}").df()
        out_path = os.path.join(output_dir, f"{table}.csv")
        df.to_csv(out_path, index=False)
        print(f"  [OK]  {table:<30}  {len(df):>6,} rows  →  {out_path}")

    con.close()

    print("=" * 55)
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="dev.duckdb", help="Path to DuckDB file")
    args = parser.parse_args()

    export_tables(args.db_path)
