"""
Minimal Athena client: run several queries concurrently and return typed rows,
plus the shared schema names and literal helpers the queries are built from.

Uses boto3 only (bundled in the Lambda Python runtime), so the function ships
without third-party dependencies.
"""

from __future__ import annotations

import time
from datetime import date, datetime

POLL_SECONDS = 1

# dbt schemas on the athena target
FINANCE = "audit_ready_dbt_finance"
SEEDS = "audit_ready_dbt"


def timestamp_literal(d: date) -> str:
    return f"timestamp '{d.isoformat()} 00:00:00'"


def between(column: str, start: date, end: date) -> str:
    """SQL predicate for column in [start, end). Bounds are date objects, so no free-form input reaches SQL."""
    return f"{column} >= {timestamp_literal(start)} and {column} < {timestamp_literal(end)}"


class AthenaQueryError(RuntimeError):
    pass


def _cast(value: str | None, athena_type: str):
    if value is None:
        return None
    if athena_type in ("double", "float", "real", "decimal"):
        return float(value)
    if athena_type in ("bigint", "integer", "int", "smallint", "tinyint"):
        return int(value)
    if athena_type.startswith("timestamp"):
        return datetime.fromisoformat(value)
    if athena_type == "date":
        return date.fromisoformat(value)
    if athena_type == "boolean":
        return value == "true"
    return value


def _fetch_rows(athena, query_id: str) -> list[dict]:
    paginator = athena.get_paginator("get_query_results")
    columns: list[tuple[str, str]] = []
    rows: list[dict] = []
    for page_no, page in enumerate(paginator.paginate(QueryExecutionId=query_id)):
        if not columns:
            columns = [(c["Name"], c["Type"]) for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        data = page["ResultSet"]["Rows"]
        # The first row of the first page repeats the column headers.
        for raw in data[1:] if page_no == 0 else data:
            values = [cell.get("VarCharValue") for cell in raw["Data"]]
            rows.append({name: _cast(v, t) for (name, t), v in zip(columns, values)})
    return rows


def run_queries(athena, queries: dict[str, str], output_location: str, workgroup: str) -> dict[str, list[dict]]:
    """Start every query at once, then wait for all of them (Athena runs them in parallel)."""
    started = {
        name: athena.start_query_execution(
            QueryString=sql,
            WorkGroup=workgroup,
            ResultConfiguration={"OutputLocation": output_location},
        )["QueryExecutionId"]
        for name, sql in queries.items()
    }
    pending = dict(started)
    while pending:
        for name, query_id in list(pending.items()):
            status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
            if status["State"] == "SUCCEEDED":
                del pending[name]
            elif status["State"] in ("FAILED", "CANCELLED"):
                raise AthenaQueryError(f"{name}: {status['State']} - {status.get('StateChangeReason')}")
        if pending:
            time.sleep(POLL_SECONDS)
    return {name: _fetch_rows(athena, query_id) for name, query_id in started.items()}
