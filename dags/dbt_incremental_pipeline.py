"""
DAG: dbt_daily_incremental
Schedule: 09:00 UTC daily

Pipeline:
  1. generate_incremental_data  — append synthetic orders for today to the parquet sources
  2. dbt_seed                   — reload audit_materiality_thresholds lookup table
  3. dbt_run_snapshot           — refresh scd_products SCD Type 2 snapshot
  4. dbt_run_intermediate       — refresh int_order_items_aggregated view
  5. dbt_run_marts              — incremental merge into revenue, order_reconciliation, refund_reconciliation
  6. dbt_test_incremental       — run dbt tests on all updated models to validate pipeline output
  7. export_for_tableau         — export mart tables to CSV for Tableau Public
"""

import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")

sys.path.insert(0, os.path.join(DBT_PROJECT_DIR, "scripts"))


def run_generate_incremental(**context) -> None:
    script_path = os.path.join(DBT_PROJECT_DIR, "scripts", "generate_daily_incremental.py")
    if not os.path.exists(script_path):
        raise FileNotFoundError(
            f"generate_daily_incremental.py not found at {script_path}. "
            "Ensure the scripts/ directory is present and mounted correctly in docker-compose.yml."
        )
    from generate_daily_incremental import generate_today_orders

    generate_today_orders(project_dir=DBT_PROJECT_DIR, n_orders=100)


def run_export_for_tableau(**context) -> None:
    from export_for_tableau import export_tables

    db_path = os.path.join(DBT_PROJECT_DIR, "dev.duckdb")
    export_tables(db_path=db_path)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="dbt_daily_incremental",
    description="Generate synthetic orders → dbt seed + snapshot → dbt incremental run → dbt test",
    schedule_interval="0 9 * * *",  # 09:00 UTC every day
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # DuckDB only supports one writer at a time
    default_args=default_args,
    tags=["dbt", "incremental", "daily"],
) as dag:
    generate_data = PythonOperator(
        task_id="generate_incremental_data",
        python_callable=run_generate_incremental,
        doc_md=(
            "Append ~100 synthetic orders for today to incr_order_items.parquet, "
            "incr_orders.parquet, and incr_inventory_items.parquet."
        ),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt seed --select audit_materiality_thresholds --profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Reload the audit_materiality_thresholds lookup table from seeds/audit_materiality_thresholds.csv. "
            "Runs daily so any threshold or risk-tier changes are applied without manual intervention."
        ),
    )

    dbt_run_snapshot = BashOperator(
        task_id="dbt_run_snapshot",
        bash_command=(
            "cd $DBT_PROJECT_DIR && dbt snapshot --select scd_products --profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Refresh the scd_products SCD Type 2 snapshot. "
            "Captures daily price / cost changes so inventory_fiscal_report can perform "
            "point-in-time product lookups at fiscal year-end."
        ),
    )

    dbt_run_int = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt run --select int_order_items_aggregated --profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md="Refresh int_order_items_aggregated view (full re-query on each run).",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt run --select revenue order_reconciliation refund_reconciliation order_item_revenue --profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Incrementally merge updated orders into the four order-level mart models. "
            "inventory_fiscal_report is excluded — year-level aggregation requires full refresh."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_incremental",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt test --select "
            "stg_incremental__order_items stg_incremental__orders stg_incremental__inventory_items "
            "int_order_items_aggregated revenue order_reconciliation refund_reconciliation order_item_revenue "
            "--profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md="Run dbt tests across all updated models to validate the daily pipeline output.",
    )

    tableau_export = PythonOperator(
        task_id="export_for_tableau",
        python_callable=run_export_for_tableau,
        doc_md=(
            "Export all five mart tables (revenue, order_reconciliation, refund_reconciliation, "
            "order_item_revenue, inventory_fiscal_report) to tableau_exports/*.csv "
            "for use in Tableau Public."
        ),
    )

    (
        generate_data
        >> dbt_seed
        >> dbt_run_snapshot
        >> dbt_run_int
        >> dbt_run_marts
        >> dbt_test
        >> tableau_export
    )
