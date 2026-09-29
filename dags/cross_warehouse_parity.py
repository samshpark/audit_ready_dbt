"""
DAG: cross_warehouse_parity
Schedule: Sundays 12:00 UTC (after the daily 09:00 run), or trigger manually

Proves the books agree to the cent on every warehouse: loads the same Parquet
source files everywhere, fully rebuilds the project on each target, then
compares journal-entry and mart totals across targets.

  1. load_sources_bigquery / load_sources_athena — reload all source tables from data/*.parquet
  2. dbt_build_<target>  — `dbt build --full-refresh` on DuckDB (a separate parity.duckdb, so
                           the daily pipeline's dev.duckdb is untouched), BigQuery, and Athena
  3. check_parity        — scripts/check_warehouse_parity.py; fails if any metric differs by
                           more than one cent (or any count differs) from the DuckDB reference

Snowflake is excluded: it ran on a 30-day trial, so a scheduled run would fail once the trial
ends. Its one-time verification is in docs/snowflake_prod_verification.md.
"""

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")

# dbt target -> profile description, in comparison order (first is the reference)
TARGETS = {
    "parity_duckdb": "DuckDB (parity.duckdb)",
    "prod": "BigQuery",
    "athena": "AWS Athena",
}

default_args = {
    "owner": "airflow",
    "retries": 0,  # a parity break should surface immediately, not be retried away
}

with DAG(
    dag_id="cross_warehouse_parity",
    description="Rebuild on DuckDB, BigQuery, and Athena from identical sources and compare the books",
    schedule_interval="0 12 * * 0",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "parity", "weekly"],
) as dag:
    env = {"DBT_PROJECT_DIR": DBT_PROJECT_DIR}

    load_bigquery = BashOperator(
        task_id="load_sources_bigquery",
        bash_command="cd $DBT_PROJECT_DIR && python scripts/load_to_bigquery.py",
        env=env,
        append_env=True,
        doc_md="Drop and reload all eight source tables in BigQuery (restarts the sandbox's 60-day expiry).",
    )

    load_athena = BashOperator(
        task_id="load_sources_athena",
        bash_command="cd $DBT_PROJECT_DIR && python scripts/load_to_s3_athena.py",
        env=env,
        append_env=True,
        doc_md="Republish all eight source files to S3 and re-register their Glue tables.",
    )

    builds = {}
    for target, label in TARGETS.items():
        builds[target] = BashOperator(
            task_id=f"dbt_build_{target}",
            bash_command=(
                "cd $DBT_PROJECT_DIR && "
                f"dbt build --target {target} --full-refresh --target-path target_{target} "
                "--profiles-dir . --no-partial-parse"
            ),
            env=env,
            append_env=True,
            doc_md=(
                f"Full-refresh build and test on {label}. Full refresh (not incremental merge) so "
                "every target is rebuilt from the same sources and no target carries merge history "
                "the others lack."
            ),
        )

    check_parity = BashOperator(
        task_id="check_parity",
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            f"python scripts/check_warehouse_parity.py --targets {' '.join(TARGETS)}"
        ),
        # dbt show writes artifacts too; keep them out of the daily pipeline's target/.
        env={**env, "DBT_TARGET_PATH": "target_parity_check"},
        append_env=True,
        doc_md=(
            "Query journal-entry debits by type, row counts, and headline mart totals on every "
            "target via `dbt show`, print a side-by-side table, and fail on any difference."
        ),
    )

    load_bigquery >> builds["prod"]
    load_athena >> builds["athena"]
    list(builds.values()) >> check_parity
