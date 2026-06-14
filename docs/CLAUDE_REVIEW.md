# dbt Project Code Review: Analytics Engineering Best Practices
> Provided below is a comprehensive summary of the AI (Claude) review, including the original input prompts and the generated outputs.

## Input Prompt:
Please conduct a deep-dive analysis of my current dbt project structure and modeling architecture based on Analytics Engineering Best Practices. Focus your review on the following five points and provide specific code examples where improvements are needed:

DAG Structure & Layering: Is the staging -> inter숮mediate -> marts flow clear? Verify if source data is sufficiently cleaned in the staging layer and check for any redundant business logic within the marts layer.

Audit-ready Integrity: Given that this project handles financial data, please ensure that primary key validations and tests are appropriately implemented across each layer to prevent data gaps or duplication.

Performance Optimization: Identify any queries causing unnecessary joins or full table scans on large datasets. Suggest where a transition to incremental models might be necessary.

Coding Conventions: Check for adherence to SQL style guides (e.g., CTE usage, naming conventions) and identify any missing descriptions for core columns in schema.yml.

Snapshot Utilization: Identify master data (e.g., product prices, exchange rates) that requires history tracking and suggest where dbt snapshots should be implemented. -- At this time, no files exist in snapshot folder. 

Once the analysis is complete, please provide the top 3 high-priority improvements first.

## Input Prompt 2: 
Please summarize the analysis and improvement suggestions you just provided and save them as a file named docs/CLAUDE_REVIEW.md. Make sure the entire content is written in professional English, as I will use it for my project documentation and technical portfolio.

# Claude Review Result: 
---

**Project:** `audit_ready_dbt`  
**Reviewer:** Claude Sonnet 4.6 (Anthropic)  
**Review Date:** 2026-04-12  
**Scope:** Full codebase review across DAG structure, data quality, performance, coding conventions, and snapshot strategy

---

## Executive Summary

This project is a well-architected financial data pipeline built on dbt + DuckDB, implementing accounting principles such as accrual-basis revenue recognition, FIFO inventory valuation, Lower of Cost or Market (LCM), and master-to-subledger reconciliation. The overall three-layer architecture (`staging → intermediate → marts`) is sound, and the emphasis on automated audit controls is commendable.

However, the review uncovered **two logic bugs that directly compromise financial data integrity**, one of which silently corrupts output column values in the most critical reporting model. Additionally, several structural improvements — including an extraction of duplicated aggregation logic and the addition of missing test coverage in the staging layer — are recommended to bring the project fully up to production-grade audit readiness.

---

## Top 3 Priority Improvements

### Priority 1 — Critical Bug: Missing Comma Corrupts Financial Output

**File:** `models/marts/fct_inventory_fiscal_report.sql`, Lines 87–88

**Problem:**

A missing comma between `period_revenue` and `beginning_inv_value` causes DuckDB to interpret the latter as a column alias for the former. As a result, the `beginning_inv_value` computed by the `LAG()` window function — which represents the prior year's ending inventory carried forward — is silently dropped from the output. Every downstream column that references `beginning_inv_value` in the `audit_check_diff` formula consequently operates on incorrect values.

```sql
-- Current (Bug): DuckDB interprets `beginning_inv_value` as an alias for `period_revenue`
period_revenue
beginning_inv_value,

-- Fixed:
period_revenue,
beginning_inv_value,
```

**Impact:**
- `audit_check_diff = (beginning_inv_value + period_purchase_amount - ending_gross_inv_value) - period_cogs_amount` silently uses `period_revenue` in place of the actual beginning inventory.
- The reconciliation check — the centerpiece of the audit-ready design — produces meaningless results.
- `inventory_turnover_ratio` is similarly affected since it depends on `beginning_inv_value`.

---

### Priority 2 — Logic Bug: Internal Control Check Always Returns 'Match'

**File:** `models/intermediate/int_inventory_items_joined.sql`, Lines 113–115

**Problem:**

The `product_match` column is intended to flag cases where the `product_id` on the inbound record (inventory items) differs from the `product_id` on the outbound record (order items). However, the implementation compares `product_id` with itself after the `COALESCE` has already merged both values into a single column. The condition `product_id <> product_id` is always `FALSE`, meaning every row unconditionally returns `'Product Match'` — rendering the internal control completely non-functional.

```sql
-- Current (Bug): self-comparison is always FALSE
CASE WHEN product_id <> product_id THEN 'Product Not Match'
    ELSE 'Product Match'
END AS product_match
```

**Fix:** Preserve the original pre-COALESCE values in the `data_cleanse` CTE, then compare them in the `final` CTE.

```sql
-- Step 1: In the data_cleanse CTE, retain both source values before merging
data_cleanse AS (
    SELECT
        COALESCE(inb.inventory_item_id, outb.inventory_item_id) AS inventory_item_id,
        COALESCE(inb.product_id, outb.product_id)               AS product_id,
        inb.product_id                                           AS inbound_product_id,  -- added
        outb.product_id                                          AS outbound_product_id, -- added
        ...
    FROM inbound inb
    FULL JOIN outbound outb ON inb.inventory_item_id = outb.inventory_item_id
),

-- Step 2: In the final CTE, compare the preserved original values
CASE
    WHEN inbound_product_id IS NOT NULL
     AND outbound_product_id IS NOT NULL
     AND inbound_product_id <> outbound_product_id
    THEN 'Product Not Match'
    ELSE 'Product Match'
END AS product_match
```

**Impact:**
- Cross-contamination between inventory items and order items (e.g., a barcode scan error) would not be detected.
- The `accepted_values` test on `product_match` always passes, providing a false sense of data integrity assurance.

---

### Priority 3 — DRY Violation: Duplicated Aggregation Logic Across Mart Models

**Files:** `models/marts/fct_revenue.sql`, `models/marts/fct_order_recon.sql`, `models/marts/fct_refund_reconciliation.sql`

**Problem:**

All three mart models independently aggregate `stg_thelook_ecommerce__order_items` to the order level, duplicating the same grouping and aggregation logic. Since all marts are materialized as tables, the `stg_thelook_ecommerce__order_items` view is scanned and fully aggregated three separate times per `dbt run`.

```sql
-- fct_revenue.sql (independent aggregation)
order_items AS (
    SELECT order_id, user_id,
           STRING_AGG(DISTINCT order_item_status) AS order_status,
           COUNT(*)                               AS num_of_item,
           ROUND(SUM(sale_price), 2)              AS tot_order_amt
    FROM {{ ref('stg_thelook_ecommerce__order_items') }}
    GROUP BY 1, 2
)

-- fct_order_recon.sql (independent, near-identical aggregation)
order_items AS (
    SELECT order_id,
           COUNT(*)                               AS num_of_item,
           ROUND(SUM(sale_price), 2)              AS tot_order_amt,
           STRING_AGG(DISTINCT order_item_status) AS order_status
    FROM {{ ref('stg_thelook_ecommerce__order_items') }}
    GROUP BY 1
)
```

**Fix:** Extract the shared logic into a new intermediate model that all three marts reference.

```sql
-- New file: models/intermediate/int_order_items_aggregated.sql
WITH order_items AS (
    SELECT * FROM {{ ref('stg_thelook_ecommerce__order_items') }}
),

final AS (
    SELECT
        order_id,
        user_id,
        STRING_AGG(DISTINCT order_item_status ORDER BY order_item_status) AS order_item_status_agg,
        COUNT(*)                                                           AS subledger_item_count,
        ROUND(SUM(sale_price), 2)                                         AS tot_order_amt,
        MIN(created_at)                                                    AS first_item_created_at,
        COUNT(CASE WHEN order_item_status = 'returned' THEN 1 END)        AS returned_item_count,
        ROUND(
            SUM(CASE WHEN order_item_status = 'returned' THEN sale_price ELSE 0 END), 2
        )                                                                  AS refund_amount
    FROM order_items
    GROUP BY 1, 2
)

SELECT * FROM final
```

**Impact:**
- Eliminates three independent full scans of `stg_thelook_ecommerce__order_items`.
- Ensures consistent aggregation logic across all downstream mart models.
- Simplifies future maintenance: a change to the aggregation logic (e.g., adding a new status) only needs to be made in one place.

---

## 1. DAG Structure & Layering

### Overall Assessment: Sound Architecture with Structural Gaps in the Order Domain

The three-layer architecture is correctly implemented for the inventory domain: `stg_thelook_ecommerce__inventory_items` feeds `int_inventory_items_joined`, which feeds `fct_inventory_fiscal_report`. This pattern — where raw data is cleaned in staging, enriched with business logic in an intermediate model, and then surfaced as a financial fact in the mart — is the correct Analytics Engineering approach.

However, the order/revenue domain bypasses the intermediate layer entirely. `fct_revenue`, `fct_order_recon`, and `fct_refund_reconciliation` all read directly from `stg_thelook_ecommerce__orders` and `stg_thelook_ecommerce__order_items`, performing their own aggregations inline. This creates the DRY violation described in Priority 3 above and makes the DAG asymmetric.

**Recommended DAG (Order Domain):**

```
stg_thelook_ecommerce__orders ──────────┐
                     ├──→ int_order_items_aggregated ──→ fct_revenue
stg_thelook_ecommerce__order_items ─────┤                           ──→ fct_order_recon
                                                  ──→ fct_refund_reconciliation
```

### Staging Layer: Timestamp Type Safety

All staging models use DuckDB's `strftime()` function to format timestamps, which returns a `VARCHAR` string rather than a `TIMESTAMP` type:

```sql
-- Current: returns VARCHAR (stg_thelook_ecommerce__orders.sql)
strftime(CAST(created_at AS TIMESTAMP), '%Y-%m-%d %H:%M:%S') AS created_at
```

This forces every downstream model to re-cast the string back to a date or timestamp type (e.g., `CAST(inbound_at AS DATE)` in `int_inventory_items_joined.sql`). Returning native `TIMESTAMP` types from the staging layer is safer and removes the redundant casting overhead.

```sql
-- Recommended: preserve native TIMESTAMP type
CAST(created_at  AS TIMESTAMP) AS created_at,
CAST(shipped_at  AS TIMESTAMP) AS shipped_at,
CAST(returned_at AS TIMESTAMP) AS returned_at,
CAST(delivered_at AS TIMESTAMP) AS delivered_at,
```

---

## 2. Data Quality & Audit Integrity

### Test Coverage by Layer

| Layer | PK Tests | FK Tests | Business Rule Tests | Documentation |
|-------|----------|----------|---------------------|---------------|
| Source (`source.yml`) | ✅ Complete | ✅ Complete | ⚠️ Partial | ✅ Good |
| Staging (`stg_*.sql`) | ❌ None | ❌ None | ❌ None | ❌ None |
| Intermediate (`int_inventory_items_joined.yml`) | ✅ Complete | ✅ Complete | ✅ Complete | ✅ Good |
| Marts (`fct_*.yml`) | ⚠️ Partial | ❌ None | ✅ Good | ⚠️ Incomplete |

### Gap 1: Staging Layer Has No Tests or Documentation

None of the staging models (`stg_thelook_ecommerce__orders`, `stg_thelook_ecommerce__order_items`, `stg_thelook_ecommerce__inventory_items`, etc.) have accompanying `.yml` files. This creates a blind spot: source tests verify the raw data, and intermediate tests verify the enriched data, but no tests confirm that the staging transformations (type casting, lowercasing, timestamp formatting) produced correct results.

A representative example of the recommended staging YAML:

```yaml
# models/staging/stg_thelook_ecommerce__order_items.yml
version: 2

models:
  - name: stg_thelook_ecommerce__order_items
    description: >
      Standardized order line items sourced from raw_order_items.
      Applies type casting (INTEGER → STRING for all IDs, DOUBLE for sale_price),
      lowercase normalization for status values, and UTC timestamp formatting.

    columns:
      - name: order_item_id
        description: "Primary key. Unique identifier for each order line item."
        tests:
          - unique
          - not_null

      - name: order_id
        description: "Foreign key to stg_thelook_ecommerce__orders."
        tests:
          - not_null
          - relationships:
              to: ref('stg_thelook_ecommerce__orders')
              field: order_id

      - name: sale_price
        description: "Actual sale price of the item at time of purchase."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              arguments:
                expression: "> 0"

      - name: order_item_status
        description: "Current lifecycle status of the item (lowercase-normalized)."
        tests:
          - accepted_values:
              arguments:
                values: ['processing', 'shipped', 'complete', 'returned', 'cancelled']
```

### Gap 2: `fct_inventory_fiscal_report` Missing Composite Primary Key Test

The grain of this model is one row per `(fiscal_year, product_id)` combination, but no test enforces this uniqueness constraint. A composite key test is essential for an audit-ready financial fact table.

```yaml
# Add to fct_inventory_fiscal_report.yml
models:
  - name: fct_inventory_fiscal_report
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - fiscal_year
            - product_id
```

### Gap 3: `fct_revenue.yml` — Core Financial Columns Undocumented

Only three of the ten columns in `fct_revenue` have descriptions or tests. The most analytically significant columns — `recognized_revenue`, `gross_revenue`, `cutoff_status`, `created_at`, and `shipped_at` — are either undocumented or untested.

```yaml
# Additions recommended for fct_revenue.yml
columns:
  - name: user_id
    description: "Customer identifier, sourced from the master orders table."
    tests:
      - not_null

  - name: recognized_revenue
    description: >
      Accrual-basis revenue recognized per order. Equals gross_revenue only when
      shipped_at is populated; zero otherwise. Implements the shipment-triggers-recognition
      revenue recognition policy.
    tests:
      - not_null
      - dbt_utils.expression_is_true:
          arguments:
            expression: ">= 0"

  - name: gross_revenue
    description: "Total order value aggregated from the subledger (stg_thelook_ecommerce__order_items), independent of shipment status."

  - name: created_at
    description: "Order creation timestamp (UTC). Used as the reference point for cut-off period risk assessment."

  - name: shipped_at
    description: "Shipment confirmation timestamp. NULL indicates a pending shipment. Drives the recognized_revenue flag."
```

### Gap 4: `raw_orders` Source — Timestamp Columns Missing Definitions

`source.yml` defines `created_at` for `raw_orders` but omits `shipped_at`, `returned_at`, and `delivered_at`. These timestamps are critical to revenue recognition and refund reconciliation.

---

## 3. Performance Optimization

### Issue 1: CROSS JOIN in `fct_inventory_fiscal_report`

```sql
-- Current: Cartesian product of all years × all products, then LEFT JOIN
FROM years y
CROSS JOIN (SELECT DISTINCT product_id, product_name, product_category, product_brand
            FROM base_ledger) p
LEFT JOIN base_ledger l ON l.product_id = p.product_id
GROUP BY 1, 2, 3, 4, 5
```

The CROSS JOIN generates N × M intermediate rows (years × distinct products) before the LEFT JOIN filters them. Since `base_ledger` contains item-level rows, this pattern joins each item back to every year-product combination before aggregating. An alternative approach is to pre-aggregate `base_ledger` to the `(product_id, fiscal_year)` grain first, and then apply the LAG window function, which avoids the Cartesian expansion entirely.

This is acceptable for the current dataset scale (~1,000 products × ~5 years = ~5,000 combinations), but would become a bottleneck at production scale with a BigQuery backend.

### Issue 2: Redundant `ORDER BY` in a TABLE-Materialized Model

```sql
-- int_inventory_items_joined.sql, last line
SELECT * FROM final
ORDER BY product_id  -- No semantic guarantee; adds unnecessary sort overhead
```

`ORDER BY` in a model materialized as a `table` does not guarantee row order for downstream consumers (SQL tables are inherently unordered sets). This clause should be removed; any required ordering should be applied at query time by the BI tool or the consuming model.

### Issue 3: Redundant Full Scans of `stg_thelook_ecommerce__order_items`

Because `fct_revenue`, `fct_order_recon`, and `fct_refund_reconciliation` are all materialized as separate tables, each `dbt run` executes three independent full scans of the `stg_thelook_ecommerce__order_items` view. Consolidating the shared aggregation into `int_order_items_aggregated` (see Priority 3) reduces this to a single scan, with the result materialized as a table for reuse.

### Incremental Model Evaluation

| Model | Incremental Suitability | Rationale |
|-------|------------------------|-----------|
| `fct_refund_reconciliation` | ⭐⭐⭐ High | Append-only by `order_id`; refund status rarely changes retroactively. |
| `fct_revenue` | ⭐⭐ Medium | Order status can change (e.g., Processing → Shipped), requiring updates to existing rows. Requires `unique_key` strategy. |
| `fct_inventory_fiscal_report` | ⭐ Low | Annual cross-year aggregation with LAG; full recalculation is simpler and safer. |

---

## 4. Coding Conventions

### Convention 1: Inconsistent File-Level Indentation

`fct_revenue.sql` has its entire content indented by four spaces, while all other models start at column zero. This appears to be an unintentional editor artifact. The `{{ config() }}` block at the top of the file and all CTEs should be aligned to the left margin for consistency.

### Convention 2: Redundant Inline `{{ config() }}` Declarations

`dbt_project.yml` already defines materialization at the folder level:

```yaml
# dbt_project.yml
models:
  audit_ready_dbt:
    staging:
      +materialized: view
    intermediate:
      +materialized: table
    marts:
      +materialized: table
```

The inline `{{ config(materialized='table') }}` declarations in `fct_revenue.sql` and `fct_order_recon.sql` are therefore redundant. Centralizing materialization in `dbt_project.yml` is the idiomatic dbt approach and avoids potential conflicts if the project-level setting is changed.

### Convention 3: Undocumented Magic Number

```sql
-- fct_inventory_fiscal_report.sql
WHEN ABS(...) <= 0.019
```

The tolerance value `0.019` has no inline explanation. For an audit-ready financial model, any numeric threshold must be documented with its derivation. 

```sql
-- Recommended: explain the tolerance basis
-- Tolerance of $0.019 accounts for floating-point rounding error
-- accumulated from ROUND(..., 2) applied to individual item-level costs.
-- Any variance exceeding this threshold is flagged for investigation.
WHEN ABS(
    (beginning_inv_value + period_purchase_amount - ending_gross_inv_value)
    - period_cogs_amount
) <= 0.019
```

### Convention 4: `fct_order_recon.yml` Missing Model-Level Description

The model YAML contains only column tests with no model-level `description` field and no descriptions for several columns (`order_id`, `user_id`, `order_at`, `tot_order_amt`, `master_item_count`, `subledger_item_count`).

---

## 5. Snapshot Strategy

### Recommended: `raw_products` Price History Snapshot

**Rationale:**

In `int_inventory_items_joined.sql`, LCM (Lower of Cost or Market) valuation uses `product_retail_price` from `stg_thelook_ecommerce__inventory_items`, which captures the retail price at the time of inventory receipt. However, the current approach has no mechanism to query what the retail price was as of a specific reporting date, which is required for precise period-end valuation adjustments.

A snapshot on `raw_products` would maintain a Type 2 SCD (Slowly Changing Dimension) history of price changes, enabling more accurate historical LCM calculations.

```sql
-- snapshots/scd_products.sql
{% snapshot scd_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['cost', 'retail_price', 'name', 'category'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    CAST(id AS STRING)           AS product_id,
    CAST(cost AS DOUBLE)         AS cost,
    CAST(retail_price AS DOUBLE) AS retail_price,
    LOWER(category)              AS category,
    LOWER(brand)                 AS brand,
    name                         AS product_name
FROM {{ source('thelook_ecommerce', 'raw_products') }}

{% endsnapshot %}
```

With this snapshot in place, `int_inventory_items_joined` can join to `scd_products` using `dbt_valid_from` / `dbt_valid_to` to retrieve the market price that was effective on any given `outbound_at` date, rather than always using the current price.

### Secondary Candidate: `raw_inventory_items` State Changes

The `sold_at` field on `raw_inventory_items` changes when an item transitions from on-hand to sold. If regulatory requirements demand a complete audit trail of inventory state changes (e.g., for GAAP/IFRS compliance), a snapshot on this table would provide the necessary historical record.

---

## Summary of All Findings

| Severity | Category | Issue | File | Line |
|----------|----------|-------|------|------|
| 🔴 Critical | Bug | Missing comma causes `beginning_inv_value` to be silently dropped from output | `fct_inventory_fiscal_report.sql` | 87 |
| 🔴 High | Bug | `product_match` self-comparison always returns 'Match'; internal control is non-functional | `int_inventory_items_joined.sql` | 113 |
| 🟠 High | Architecture | Three mart models independently aggregate `stg_thelook_ecommerce__order_items`; no intermediate model | `fct_revenue.sql`, `fct_order_recon.sql`, `fct_refund_reconciliation.sql` | — |
| 🟠 High | Data Quality | No YAML tests or documentation exist for any staging model | `stg_*.sql` | — |
| 🟡 Medium | Data Quality | Composite PK test `(fiscal_year, product_id)` missing on `fct_inventory_fiscal_report` | `fct_inventory_fiscal_report.yml` | — |
| 🟡 Medium | Data Quality | `fct_revenue.yml` missing descriptions and tests for seven core financial columns | `fct_revenue.yml` | — |
| 🟡 Medium | Data Quality | `fct_order_recon.yml` missing model-level description and column descriptions | `fct_order_recon.yml` | — |
| 🟡 Medium | Performance | Redundant `ORDER BY` in a TABLE-materialized intermediate model | `int_inventory_items_joined.sql` | 123 |
| 🟡 Medium | Performance | Three independent full scans of `stg_thelook_ecommerce__order_items` per dbt run | Multiple mart models | — |
| 🟢 Low | Convention | `strftime()` in staging returns VARCHAR; downstream re-casting is error-prone | All `stg_*.sql` | — |
| 🟢 Low | Convention | Inline `{{ config(materialized='table') }}` is redundant with `dbt_project.yml` | `fct_revenue.sql`, `fct_order_recon.sql` | — |
| 🟢 Low | Convention | Tolerance value `0.019` is a magic number without documented derivation | `fct_inventory_fiscal_report.sql` | 101 |
| 🟢 Low | Enhancement | `raw_products` price history not captured; LCM uses current price rather than period-end price | Snapshot not implemented | — |

---

*This review was conducted against the `main` branch as of commit `1dd7eda`. All line number references are relative to the file state at that commit.*
