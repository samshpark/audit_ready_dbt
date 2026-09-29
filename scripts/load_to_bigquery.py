"""
Local Parquet -> BigQuery Loader (BigQuery source layer)

Purpose:
    Load the local data/*.parquet source files into BigQuery so
    `dbt build --target prod` reads the same source data as the DuckDB and
    Athena targets. Used by the cross_warehouse_parity DAG before each
    comparison run.

Layout:
    <project>.<source>.<table> -- dataset = dbt source name (thelook_ecommerce,
    incremental), which is the schema dbt resolves source() to by default.

Table expiration:
    The project's datasets carry a 60-day default table expiration (BigQuery
    sandbox), and a WRITE_TRUNCATE load keeps a table's original expiry. Each
    table is therefore dropped and recreated, which restarts its 60 days.

Usage:
    python scripts/load_to_bigquery.py [--source incremental]
"""

import argparse
import io
import sys

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.oauth2 import service_account
from source_files import SOURCES, to_parquet_bytes

KEY_PATH = "credentials/google_creds.json"
LOCATION = "US"


def load_table(client: bigquery.Client, source: str, table: str) -> None:
    body, _ = to_parquet_bytes(f"data/{table}.parquet")
    table_id = f"{client.project}.{source}.{table}"
    client.delete_table(table_id, not_found_ok=True)
    job = client.load_table_from_file(
        io.BytesIO(body),
        table_id,
        location=LOCATION,
        job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET),
    )
    job.result()
    print(f"  {source}.{table}: {client.get_table(table_id).num_rows:,} rows")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=sorted(SOURCES), help="Load only this dbt source")
    args = parser.parse_args()

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    try:
        for source, tables in SOURCES.items():
            if args.source and source != args.source:
                continue
            client.create_dataset(bigquery.Dataset(f"{client.project}.{source}"), exists_ok=True)
            print(f"{source}:")
            for table in tables:
                load_table(client, source, table)
        return 0
    except GoogleAPIError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
