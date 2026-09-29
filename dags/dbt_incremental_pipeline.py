"""
DAG: dbt_daily_incremental
Schedule: 09:00 UTC daily

Pipeline — after step 1 the DAG forks into a local DuckDB branch (feeds the Tableau
exports) and an AWS branch (the production Athena warehouse and the journal-entry export):

  1. generate_incremental_data  — append synthetic orders for today to the parquet sources

  DuckDB branch
  2. dbt_source_freshness       — check incr_* source freshness (see caveat on the task below —
                                  this always passes here, since it runs right after the step
                                  that just wrote today's data)
  3. dbt_seed                   — reload the audit_materiality_thresholds, chart_of_accounts, and
                                  journal_entry_rules lookup tables
  4. dbt_run_snapshot           — refresh scd_products SCD Type 2 snapshot
  5. dbt_run_intermediate       — recreate all five intermediate views (safety net for view
                                  definitions; views already reflect current data without this)
  6. dbt_run_marts              — incremental merge into revenue, order_reconciliation,
                                  refund_reconciliation, order_item_revenue (full-refresh instead on
                                  Sundays, to catch backdated corrections the lookback window can't
                                  see); full-refresh rebuild of inventory_fiscal_report,
                                  inventory_sellthrough, journal_entry_lines, journal_entries
  7. dbt_test_incremental       — run dbt tests on all updated models to validate pipeline output
  8. export_for_tableau         — export mart tables to CSV for Tableau Public

  AWS branch
  a. upload_incremental_to_s3   — republish incr_*.parquet to S3 / Glue (Athena source layer)
  b. dbt_build_athena           — dbt build on Athena: models + all tests, including the journal
                                  balance and control-total reconciliation tests
  c. export_journal_entries     — invoke the je_pipeline Lambda for the run's business date:
                                  GL upload file, support detail, and exception report to S3
"""

import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
JE_PIPELINE_FUNCTION = os.environ.get("JE_PIPELINE_FUNCTION", "audit-ready-je-pipeline")
AWS_REGION = "us-east-1"

sys.path.insert(0, os.path.join(DBT_PROJECT_DIR, "scripts"))


def run_generate_incremental(**context) -> None:
    script_path = os.path.join(DBT_PROJECT_DIR, "scripts", "generate_daily_incremental.py")
    if not os.path.exists(script_path):
        raise FileNotFoundError(
            f"generate_daily_incremental.py not found at {script_path}. "
            "Ensure the scripts/ directory is present and mounted correctly in docker-compose.yml."
        )
    from generate_daily_incremental import generate_today_orders

    generate_today_orders(project_dir=DBT_PROJECT_DIR)


def run_je_pipeline(ds: str, **context) -> None:
    """Invoke the Lambda synchronously for the business date and fail the task if it errors."""
    import json

    import boto3

    response = boto3.client("lambda", region_name=AWS_REGION).invoke(
        FunctionName=JE_PIPELINE_FUNCTION,
        Payload=json.dumps({"start_date": ds}),
    )
    payload = response["Payload"].read().decode()
    if response.get("FunctionError"):
        raise RuntimeError(f"{JE_PIPELINE_FUNCTION} failed: {payload}")
    print(payload)


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
            "Append synthetic orders for today to incr_order_items.parquet, "
            "incr_orders.parquet, and incr_inventory_items.parquet. Daily count "
            "starts at ~15 and grows ~5%/month from the BigQuery ingestion cutoff, "
            "continuing that source's own growth trend instead of flatlining."
        ),
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt source freshness --select source:incremental --profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Check incr_orders / incr_order_items freshness (warn_after: 30h, error_after: 54h, "
            "sized to the daily 09:00 UTC cadence). Caveat: placed right after "
            "generate_incremental_data, this only ever checks data that step just wrote, so it "
            "passes trivially whenever the DAG runs at all — it can't detect the actual failure "
            "mode that matters here (the scheduler not running because the host machine is off), "
            "since nothing runs to report that. Kept as a demonstration of the mechanism, not a "
            "real freshness SLA — that would require always-on infrastructure this local Airflow "
            "instance doesn't have."
        ),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt seed --select audit_materiality_thresholds chart_of_accounts journal_entry_rules "
            "--profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Reload the lookup seeds: audit_materiality_thresholds, plus chart_of_accounts and "
            "journal_entry_rules for the journal-entry models. Runs daily so threshold, risk-tier, or "
            "posting-rule changes are applied without manual intervention."
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
            "dbt run --select int_order_items_unioned int_order_items_aggregated int_orders_joined "
            "int_inventory_items_joined int_inventory_items_unioned "
            "--profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Recreate all five intermediate views. These are views, not tables, so they already "
            "reflect current data on every query without this step — it exists only as a safety "
            "net that keeps view definitions in sync if a model's SQL changes."
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt run --select revenue order_reconciliation refund_reconciliation order_item_revenue "
            "inventory_fiscal_report inventory_sellthrough journal_entry_lines journal_entries "
            "{% if dag_run.logical_date.weekday() == 6 %}--full-refresh{% endif %} "
            "--profiles-dir . --target dev --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Incrementally merge updated orders into the four order-level mart models. "
            "inventory_fiscal_report, inventory_sellthrough, and the two journal-entry models are plain "
            "table materializations, "
            "so this fully rebuilds them each run rather than merging — cross-year LAG logic and "
            "the unit-level sell-through view both need complete recalculation, not a partial merge.\n\n"
            "On Sundays, `--full-refresh` is added so the four incremental models rebuild from "
            "scratch too — the lookback window only re-scans recent business timestamps (14 days), so a "
            "backdated correction older than the window (e.g. a late data-entry fix to an order "
            "from months ago) would otherwise never get picked back up by the merge."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_incremental",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt test --select "
            "stg_incremental__order_items stg_incremental__orders stg_incremental__inventory_items "
            "int_order_items_unioned int_order_items_aggregated int_orders_joined "
            "int_inventory_items_joined int_inventory_items_unioned "
            "revenue order_reconciliation refund_reconciliation order_item_revenue "
            "inventory_fiscal_report inventory_sellthrough journal_entry_lines journal_entries "
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
            "Export all six mart tables (revenue, order_reconciliation, refund_reconciliation, "
            "order_item_revenue, inventory_fiscal_report, inventory_sellthrough) to tableau_exports/*.csv "
            "for use in Tableau Public."
        ),
    )

    upload_to_s3 = BashOperator(
        task_id="upload_incremental_to_s3",
        bash_command="cd $DBT_PROJECT_DIR && python scripts/load_to_s3_athena.py --source incremental",
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Republish today's incr_*.parquet to S3 and re-register the Glue tables, so the Athena "
            "source layer matches the files the DuckDB branch reads."
        ),
    )

    dbt_build_athena = BashOperator(
        task_id="dbt_build_athena",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt build --target athena --target-path target_athena "
            "{% if dag_run.logical_date.weekday() == 6 %}--full-refresh{% endif %} "
            "--profiles-dir . --no-partial-parse"
        ),
        env={"DBT_PROJECT_DIR": DBT_PROJECT_DIR},
        append_env=True,
        doc_md=(
            "Build and test every model on Athena (Iceberg merge for the incremental marts, "
            "full refresh on Sundays as in the DuckDB branch). A failing test -- including "
            "assert_journal_entries_balanced or assert_journal_entries_reconcile_to_marts -- stops "
            "the branch before any journal entry is exported. Uses its own --target-path so it "
            "never collides with the DuckDB branch's target/ artifacts."
        ),
    )

    export_journal_entries = PythonOperator(
        task_id="export_journal_entries",
        python_callable=run_je_pipeline,
        doc_md=(
            "Invoke the je_pipeline Lambda for the run's business date (`ds`). It exports the "
            "day's journal_entries as a GL upload CSV with order/item-level support, re-checks "
            "debit = credit at the boundary, runs the exception rules, and writes everything to S3."
        ),
    )

    generate_data >> upload_to_s3 >> dbt_build_athena >> export_journal_entries

    (
        generate_data
        >> dbt_source_freshness
        >> dbt_seed
        >> dbt_run_snapshot
        >> dbt_run_int
        >> dbt_run_marts
        >> dbt_test
        >> tableau_export
    )
