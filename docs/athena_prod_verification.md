# AWS Athena — Build Verification

Evidence that the models compile and run correctly against AWS (S3 + Glue
Data Catalog + Athena), not just DuckDB/BigQuery/Snowflake — supporting the
Multi-Environment claim in the [README](../README.md#2-tech-stack--engineering-value).

- **Date**: 2026-09-29
- **Command**: `dbt build --target athena`
- **dbt version**: 1.10.20 (`dbt-athena` 1.9.5)
- **Elapsed time**: 191.2s (Athena is serverless — each statement carries a
  few seconds of query-queue overhead regardless of data size, so views take
  ~5s here versus <1s on the other targets)
- **Raw output**: [`athena_prod_run_results.raw.json`](athena_prod_run_results.raw.json) — full dbt `run_results.json` artifact (193 nodes), local filesystem paths masked

## Result

| Status | Count |
|---|---|
| pass | 166 |
| success | 22 |
| warn | 1 |
| no-op | 4 |
| **error** | **0** |

Identical to the BigQuery and Snowflake runs. The single `warn` is the
expected `assert_fulfillment_lead_time_within_baseline` test (see README §6) —
a known source-data defect the test is designed to flag, not a build failure.

## AWS architecture

```
data/*.parquet
   │  scripts/load_to_s3_athena.py  (boto3)
   ▼
s3://audit-ready-dbt-raw-*/<source>/<table>/        ← Glue external tables
   │                                                  (thelook_ecommerce, incremental)
   ▼
Athena (engine v3, Trino)  ◀── dbt-athena
   │
   ▼
s3://audit-ready-dbt-athena-*/tables/               ← dbt models (Iceberg)
s3://audit-ready-dbt-athena-*/query-results/        ← 7-day lifecycle expiry
```

- **IAM**: a dedicated `dbt-athena` user with an inline least-privilege
  policy — S3 access is scoped to `audit-ready-dbt-*` buckets only; no root
  credentials are used. Credentials live in a named AWS CLI profile
  (`aws_profile_name` in `profiles.yml`), never in the repo.
- **Source layer**: `scripts/load_to_s3_athena.py` uploads each Parquet file
  and registers it as a Glue external table named after its dbt source, so
  `source()` resolves unchanged. Row counts were checked against the local
  files after upload.

## Athena-specific portability fixes

This run surfaced three Trino-only incompatibilities, each fixed in a way
that keeps the other three targets unchanged:

1. **`cast(x as string)`** — Trino has no `string` type. The 21 staging casts
   now use dbt's cross-database `{{ dbt.type_string() }}` (renders `string` on
   BigQuery/DuckDB, `TEXT` on Snowflake, `varchar` on Athena).
2. **`fiscal_year_end()`** — Trino rejects `bigint || varchar`, which the
   other engines coerce implicitly. The year is now cast to string before
   concatenation.
3. **`string_agg_distinct()`** — Trino has neither `string_agg` nor
   `listagg ... within group`. Added an Athena branch:
   `array_join(array_sort(array_distinct(array_agg(col))), ',')`.

In addition, Athena cannot `MERGE` into Hive-format tables, so
`dbt_project.yml` sets `+table_type: iceberg` for models and snapshots (a
key the other adapters ignore). This lets the four incremental marts keep
`incremental_strategy='merge'` on every target.

## Incremental (merge) verification

A full build only creates the incremental marts, so the merge path was
verified separately:

1. `scripts/generate_daily_incremental.py` appended the day's synthetic
   orders (18 orders / 34 items), then
   `load_to_s3_athena.py --source incremental` republished them to S3.
2. `dbt build --target athena --select config.materialized:incremental`
   merged the new and in-lookback rows (`revenue` 23, `order_reconciliation`
   23, `refund_reconciliation` 18, `order_item_revenue` 46) — 38/38 tests
   passed.
3. Post-merge, every mart's row count equals its distinct-key count
   (e.g. `revenue` 13,151 / 13,151), confirming the merge upserted rather
   than duplicated.

## Build times (models, seed, snapshot)

The 167 individual data tests are omitted below for brevity — all passed
except the one warn noted above.

| Name | Kind | Status | Time (s) |
|---|---|---|---|
| audit_materiality_thresholds | seed | success | 21.06 |
| refund_reconciliation | model | success | 11.90 |
| metricflow_time_spine | model | success | 11.23 |
| order_reconciliation | model | success | 10.67 |
| revenue | model | success | 10.47 |
| order_item_revenue | model | success | 10.47 |
| inventory_fiscal_report | model | success | 10.28 |
| inventory_sellthrough | model | success | 6.61 |
| scd_products | snapshot | success | 6.37 |
| stg_incremental__order_items | model | success | 5.61 |
| int_inventory_items_unioned | model | success | 5.49 |
| int_inventory_items_joined | model | success | 5.46 |
| int_order_items_unioned | model | success | 5.40 |
| int_order_items_aggregated | model | success | 5.30 |
| stg_incremental__orders | model | success | 5.26 |
| stg_incremental__inventory_items | model | success | 5.10 |
| stg_thelook_ecommerce__orders | model | success | 5.06 |
| stg_thelook_ecommerce__products | model | success | 4.97 |
| stg_thelook_ecommerce__inventory_items | model | success | 4.96 |
| int_orders_joined | model | success | 4.86 |
| stg_thelook_ecommerce__order_items | model | success | 4.82 |
| stg_thelook_ecommerce__users | model | success | 4.77 |
