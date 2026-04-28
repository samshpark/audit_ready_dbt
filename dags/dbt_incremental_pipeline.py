"""
DAG: dbt_daily_incremental
Schedule: 09:00 UTC daily

Pipeline:
  1. generate_incremental_data  — append synthetic orders for today to the parquet sources
  2. dbt_run_incremental        — merge new rows into int_order_items_summary (incremental)
  3. dbt_test_incremental       — run dbt tests on the updated model
"""

import os
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")

# Add the dbt project's scripts/ to the Python path so we can import directly
sys.path.insert(0, os.path.join(DBT_PROJECT_DIR, "scripts"))


def run_generate_incremental(**context) -> None:
    from generate_daily_incremental import generate_today_orders
    generate_today_orders(project_dir=DBT_PROJECT_DIR, n_orders=100)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_daily_incremental",
    description="Generate synthetic orders → dbt incremental run → dbt test",
    schedule_interval="0 9 * * *",   # 09:00 UTC every day
    start_date=datetime(2026, 4, 22),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "incremental", "daily"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_incremental_data",
        python_callable=run_generate_incremental,
        doc_md="Append ~100 synthetic orders for today to raw_order_items.parquet and raw_orders.parquet.",
    )

    dbt_run_int = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt run --select int_order_items_summary --profiles-dir . --target dev"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md="Merge today's new order rows into int_order_items_summary (incremental).",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt run --select fct_revenue fct_order_recon fct_refund_reconciliation --profiles-dir . --target dev"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Incrementally merge updated orders into the three order-level mart models. "
            "fct_inventory_fiscal_report is excluded — year-level aggregation requires full refresh."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_incremental",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt test --select int_order_items_summary fct_revenue fct_order_recon fct_refund_reconciliation --profiles-dir . --target dev"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md="Run dbt tests across all four incremental models to validate the daily pipeline output.",
    )

    generate_data >> dbt_run_int >> dbt_run_marts >> dbt_test
