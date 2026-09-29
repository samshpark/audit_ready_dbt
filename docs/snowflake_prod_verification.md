# Snowflake — Build Verification

Evidence that the models compile and run correctly against a live Snowflake
warehouse, not just DuckDB/BigQuery — supporting the Multi-Environment claim
in the [architecture doc](architecture.md#2-high-performance-local-development).

- **Date**: 2026-09-13
- **Command**: `dbt build --target snowflake`
- **dbt version**: 1.10.20 (`dbt-snowflake` 1.10.2)
- **Elapsed time**: 20.0s warm (an initial cold start, with the `XSMALL`
  warehouse auto-resuming from `AUTO_SUSPEND`, took ~5m)
- **Raw output**: [`snowflake_prod_run_results.raw.json`](snowflake_prod_run_results.raw.json) — full dbt `run_results.json` artifact (193 nodes), local filesystem paths masked

## Result

| Status | Count |
|---|---|
| pass | 166 |
| success | 22 |
| warn | 1 |
| no-op | 4 |
| **error** | **0** |

The single `warn` is the expected `assert_fulfillment_lead_time_within_baseline`
test (see [Quality Control](architecture.md#6-quality-control)) — a known source-data defect the test is designed to
flag, not a build failure.

## Snowflake-specific portability fix

Unlike the BigQuery pass, this run surfaced one genuine Snowflake-only SQL
incompatibility: `string_agg(distinct col order by col)` — valid on both
DuckDB and BigQuery — has no Snowflake equivalent. Snowflake requires
`listagg(distinct col, sep) within group (order by col)` instead. Fixed by
adding a dispatch macro, [`string_agg_distinct()`](../macros/string_agg_distinct.sql),
used in `int_order_items_aggregated`.

## Build times (models, seed, snapshot)

The 167 individual data tests are omitted below for brevity — all passed
except the one warn noted above.

| Name | Kind | Status | Time (s) |
|---|---|---|---|
| scd_products | snapshot | success | 3.19 |
| order_reconciliation | model | success | 3.05 |
| revenue | model | success | 3.02 |
| refund_reconciliation | model | success | 2.42 |
| order_item_revenue | model | success | 2.24 |
| inventory_sellthrough | model | success | 1.75 |
| inventory_fiscal_report | model | success | 1.71 |
| audit_materiality_thresholds | seed | success | 1.41 |
| metricflow_time_spine | model | success | 1.35 |
| stg_thelook_ecommerce__products | model | success | 0.57 |
| int_orders_joined | model | success | 0.49 |
| int_inventory_items_unioned | model | success | 0.44 |
| int_inventory_items_joined | model | success | 0.40 |
| stg_incremental__inventory_items | model | success | 0.32 |
| int_order_items_unioned | model | success | 0.32 |
| int_order_items_aggregated | model | success | 0.32 |
| stg_thelook_ecommerce__orders | model | success | 0.31 |
| stg_incremental__order_items | model | success | 0.30 |
| stg_incremental__orders | model | success | 0.30 |
| stg_thelook_ecommerce__users | model | success | 0.28 |
| stg_thelook_ecommerce__inventory_items | model | success | 0.24 |
| stg_thelook_ecommerce__order_items | model | success | 0.21 |
