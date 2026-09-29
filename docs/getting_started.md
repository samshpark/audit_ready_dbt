# Getting Started

**Prerequisites**: Docker Desktop, Python 3.9+, Google Cloud account (free tier — thelook_ecommerce is a public dataset)

> **Note for reviewers**: `data/raw_*.parquet` (~4 MB, BigQuery source data) is included in this repository. **Steps 4–5 (BigQuery ingestion) can be skipped**. From Step 7, choose either manual execution or Airflow — `incr_*.parquet` files are excluded from git as they change daily.

## Step 1 — Clone the repository
```bash
git clone https://github.com/samshpark/audit_ready_dbt.git
cd audit_ready_dbt
```

## Step 2 — Create `profiles.yml`
`profiles.yml` is excluded from git. Create it manually in the project root:
```yaml
audit_ready_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dev.duckdb
    prod:
      type: bigquery
      method: service-account
      project: your-gcp-project-id
      dataset: audit_ready_dbt
      keyfile: credentials/google_creds.json
      threads: 4
      timeout_seconds: 300
      location: US
```

## Step 3 — Set up local Python environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install dbt-duckdb dbt-bigquery dbt-metricflow google-cloud-bigquery pandas pyarrow
```
> `dbt-bigquery` is only required if you intend to run against the BigQuery `prod` target.

## Step 4 — Set up BigQuery credentials
- Create a [Google Cloud service account](https://console.cloud.google.com/iam-admin/serviceaccounts) with **BigQuery Data Viewer** and **BigQuery Job User** roles
- Download the JSON key and save it to `credentials/google_creds.json` (excluded from git)

## Step 5 — Ingest data from BigQuery *(optional — raw_*.parquet already in repo)*
```bash
# Only needed if you want to re-pull fresh data from BigQuery
python scripts/ingest_data.py
```

## Step 6 — Install dbt packages *(required)*
```bash
dbt deps
```

## Step 7 — Run the pipeline

From Step 7 onwards, you can either run the pipeline **manually** or let **Airflow** handle it.

### Option A — Manual
```bash
python scripts/generate_daily_incremental.py --reset --backfill-from 2025-06-01  # initialize incr_*.parquet
dbt seed                                       # load audit_materiality_thresholds
dbt snapshot                                   # build scd_products price history
dbt run                                        # execute all models
dbt test                                       # validate all tests
python scripts/export_for_tableau.py           # export mart tables to tableau_exports/*.csv
```

### Option B — Airflow
```bash
# Builds the Docker image and starts Postgres, webserver, and scheduler
docker-compose up -d

# Wait ~30 seconds for containers to become healthy, then verify
docker ps
```
Open **http://localhost:8080** and log in with `admin` / `admin`.
Enable the `dbt_daily_incremental` DAG — it runs automatically at 09:00 UTC daily, or trigger it manually from the UI.

The DAG handles `generate_daily_incremental.py → dbt seed → dbt snapshot → dbt run intermediate → dbt run marts → dbt test → export_for_tableau` on every run.

### Option C — AWS (Athena + journal-entry Lambda) *(optional)*
Requires an AWS account and two IAM users: one for the pipeline (S3/Glue/Athena access plus `lambda:InvokeFunction` on the function) and one for deploys. Add an `athena` output to `profiles.yml`:
```yaml
    athena:
      type: athena
      aws_profile_name: <your-pipeline-profile>
      region_name: us-east-1
      s3_staging_dir: s3://<athena-bucket>/query-results/
      s3_data_dir: s3://<athena-bucket>/tables/
      database: awsdatacatalog
      schema: audit_ready_dbt
      work_group: primary
      threads: 4
```
```bash
python scripts/load_to_s3_athena.py          # publish sources to S3 + Glue
dbt build --target athena                    # build and test every model on Athena
cd lambdas && sam build && sam deploy        # deploy the journal-entry Lambda (see samconfig.toml)
```
Bucket names are defined in `scripts/load_to_s3_athena.py` and `lambdas/template.yaml` — change them to globally unique names for your account. The Airflow AWS branch and the parity DAG then work as-is; the parity DAG also expects a `parity_duckdb` DuckDB output (`path: parity.duckdb`) in `profiles.yml`.

## Step 8 — Query metrics via Semantic Layer
```bash
# Validate semantic model definitions
mf validate-configs
```

See [Semantic Layer — Example Queries](semantic_layer.md#example-queries) for `mf query` usage.

## Step 9 — Browse dbt documentation *(optional)*
```bash
dbt docs generate
dbt docs serve
```
Open **http://localhost:8080** to explore model metadata, column descriptions, data tests, and the full dependency graph.
