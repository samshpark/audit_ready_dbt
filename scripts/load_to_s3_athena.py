"""
Local Parquet -> S3 + Glue Data Catalog Loader (Athena source layer)

Purpose:
    Publish the local data/*.parquet source files to S3 and register each one
    as an external table in the Glue Data Catalog, so `dbt build --target athena`
    reads the same source data as the DuckDB, BigQuery, and Snowflake targets.

Layout:
    s3://<raw bucket>/<source>/<table>/<table>.parquet
    Glue database = dbt source name (thelook_ecommerce, incremental), which is
    the schema dbt resolves source() to by default.

Type handling:
    Timestamps are rewritten at microsecond precision before upload (see
    source_files.to_parquet_bytes).

Usage:
    AWS_PROFILE=audit-ready-dbt python scripts/load_to_s3_athena.py
"""

import argparse
import sys
import time

import boto3
import pyarrow as pa
from botocore.exceptions import ClientError
from source_files import SOURCES, to_parquet_bytes

REGION = "us-east-1"
RAW_BUCKET = "audit-ready-dbt-raw-sam2026"
ATHENA_RESULTS = "s3://audit-ready-dbt-athena-sam2026/query-results/"


def athena_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    if pa.types.is_int64(arrow_type):
        return "bigint"
    if pa.types.is_floating(arrow_type):
        return "double"
    if pa.types.is_string(arrow_type):
        return "string"
    raise ValueError(f"Unmapped Arrow type: {arrow_type}")


def run_query(athena, sql: str) -> None:
    query_id = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS},
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {status['State']}: {status.get('StateChangeReason')}\n{sql}")


def load_table(s3, athena, source: str, table: str) -> None:
    body, schema = to_parquet_bytes(f"data/{table}.parquet")
    prefix = f"{source}/{table}/"
    s3.put_object(Bucket=RAW_BUCKET, Key=f"{prefix}{table}.parquet", Body=body)

    columns = ",\n  ".join(f"`{f.name}` {athena_type(f.type)}" for f in schema)
    run_query(athena, f"DROP TABLE IF EXISTS `{source}`.`{table}`")
    run_query(
        athena,
        f"CREATE EXTERNAL TABLE `{source}`.`{table}` (\n  {columns}\n)\n"
        f"STORED AS PARQUET\nLOCATION 's3://{RAW_BUCKET}/{prefix}'",
    )
    print(f"  {source}.{table}: {len(body) / 1024:,.0f} KiB -> s3://{RAW_BUCKET}/{prefix}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--source",
        choices=sorted(SOURCES),
        help="Load only this dbt source (e.g. 'incremental' after the daily Airflow run)",
    )
    args = parser.parse_args()

    session = boto3.Session(region_name=REGION)
    s3 = session.client("s3")
    athena = session.client("athena")

    try:
        for source, tables in SOURCES.items():
            if args.source and source != args.source:
                continue
            run_query(athena, f"CREATE DATABASE IF NOT EXISTS `{source}`")
            print(f"{source}:")
            for table in tables:
                load_table(s3, athena, source, table)
        return 0
    except (ClientError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
