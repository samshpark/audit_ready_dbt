{{ config(
    tags=['financial', 'audit_ready']
) }}

WITH base_ledger AS (
    SELECT * FROM {{ ref('int_inventory_items_joined') }}
),

-- 1. get all available fiscal year
years AS (
    SELECT DISTINCT inbound_fiscal_year AS fiscal_year FROM base_ledger WHERE inbound_fiscal_year IS NOT NULL
    UNION
    SELECT DISTINCT outbound_fiscal_year FROM base_ledger WHERE outbound_fiscal_year IS NOT NULL
),

-- 2. By Fiscal Year & Product: Detail Metrics
product_year_flow AS (
    SELECT
        y.fiscal_year,
        l.product_id,
        l.product_name,
        l.product_category,
        l.product_brand,
        
        -- [P/L] Sold for the fiscal_year
        COUNT(CASE WHEN l.outbound_fiscal_year = y.fiscal_year THEN l.inventory_item_id END) AS period_units_sold,
        ROUND(SUM(CASE WHEN l.outbound_fiscal_year = y.fiscal_year THEN l.historical_unit_cost ELSE 0 END), 2) AS period_cogs_amount,
        ROUND(SUM(CASE WHEN l.outbound_fiscal_year = y.fiscal_year THEN l.sale_price ELSE 0 END), 2) AS period_revenue,

        -- [B/S] On hand inventory: as of the fiscal year end
        COUNT(CASE 
            WHEN l.inbound_fiscal_year <= y.fiscal_year 
            AND (l.outbound_fiscal_year IS NULL OR l.outbound_fiscal_year > y.fiscal_year) 
            THEN l.inventory_item_id END) AS ending_inv_qty,
        
        ROUND(SUM(CASE 
            WHEN l.inbound_fiscal_year <= y.fiscal_year 
            AND (l.outbound_fiscal_year IS NULL OR l.outbound_fiscal_year > y.fiscal_year) 
            THEN l.historical_unit_cost ELSE 0 END), 2) AS ending_gross_inv_value,

        -- [LCM (Low Cost Market) & NRV (Net realizable value)]: as of the fiscal year end
        -- Uses period-end market price from scd_products snapshot (fiscal year Dec 31),
        -- falling back to the inbound-time price from stg_inventory_items if no snapshot record exists.
        ROUND(SUM(CASE
            WHEN l.inbound_fiscal_year <= y.fiscal_year
            AND (l.outbound_fiscal_year IS NULL OR l.outbound_fiscal_year > y.fiscal_year)
            THEN GREATEST(0, l.historical_unit_cost - LEAST(l.historical_unit_cost, COALESCE(scd.retail_price, l.current_market_price)))
            ELSE 0 END), 2) AS ending_allowance_lcm,

        ROUND(SUM(CASE
            WHEN l.inbound_fiscal_year <= y.fiscal_year
            AND (l.outbound_fiscal_year IS NULL OR l.outbound_fiscal_year > y.fiscal_year)
            THEN LEAST(l.historical_unit_cost, COALESCE(scd.retail_price, l.current_market_price))
            ELSE 0 END), 2) AS ending_net_realizable_value,

        -- [Period Purchase Amount]: for the fiscal year
        ROUND(SUM(CASE WHEN l.inbound_fiscal_year = y.fiscal_year THEN l.historical_unit_cost ELSE 0 END), 2) AS period_purchase_amount,

        -- [Operational: Aging] Average days in inventory as of the fiscal year end: Fiscal Year End date - inbound date
        ROUND(AVG(CASE 
            WHEN l.inbound_fiscal_year <= y.fiscal_year 
            AND (l.outbound_fiscal_year IS NULL OR l.outbound_fiscal_year > y.fiscal_year) 
            THEN ({{ fiscal_year_end('y.fiscal_year') }} - CAST(l.inbound_at AS DATE))
        END), 1) AS avg_days_on_hand_at_year_end

    FROM years y
    CROSS JOIN base_ledger l
    LEFT JOIN {{ ref('scd_products') }} scd
        ON  l.product_id = scd.product_id
        AND {{ fiscal_year_end('y.fiscal_year') }} >= scd.dbt_valid_from
        AND (scd.dbt_valid_to IS NULL OR {{ fiscal_year_end('y.fiscal_year') }} < scd.dbt_valid_to)
    GROUP BY 1, 2, 3, 4, 5
),

final_report AS (
    SELECT
        *,
        -- Beginning Inventory = Ending Inventory of Last Fiscal Year
        LAG(ending_gross_inv_value, 1, 0) OVER (PARTITION BY product_id ORDER BY fiscal_year) AS beginning_inv_value
    FROM product_year_flow
),

final AS (

    SELECT
        fr.fiscal_year,
        fr.product_id,
        fr.product_name,
        fr.product_category,
        fr.product_brand,
        fr.period_units_sold,
        fr.period_cogs_amount,
        fr.period_revenue,
        fr.beginning_inv_value,
        fr.period_purchase_amount,
        fr.ending_gross_inv_value,
        fr.ending_inv_qty,
        fr.ending_allowance_lcm,
        fr.ending_net_realizable_value,
        fr.avg_days_on_hand_at_year_end,

        -- 1. Inventory Reconciliation & Audit Check
            -- Formula: (Beginning + Purchase - Ending) - COGS
            -- This should ideally be 0. Discrepancies indicate:
                --   (+) Positive: Inventory Shrinkage (Unrecorded loss, theft, or breakage)
                --   (-) Negative: Surplus/Ghost Inventory (Unrecorded receipts or COGS overstatement)

        CASE
            WHEN ABS((fr.beginning_inv_value + fr.period_purchase_amount - fr.ending_gross_inv_value) - fr.period_cogs_amount) <= 0.019
            -- Tolerance of $0.019 accounts for floating-point rounding error accumulated from
            -- ROUND(..., 2) applied to individual item-level costs across multiple aggregations.
            -- Any variance exceeding this threshold is a real discrepancy and flagged for investigation.
            THEN 0
            ELSE ROUND((fr.beginning_inv_value + fr.period_purchase_amount - fr.ending_gross_inv_value) - fr.period_cogs_amount, 2)
        END AS audit_check_diff,

        -- 2. Financial Ratios
        ROUND(fr.period_cogs_amount / NULLIF((fr.beginning_inv_value + fr.ending_gross_inv_value) / 2, 0), 3) AS inventory_turnover_ratio,
        ROUND((fr.period_revenue - fr.period_cogs_amount) / NULLIF(fr.period_revenue, 0), 4) AS gross_profit_margin,

        -- 3. Risk Assessment
        CASE
            WHEN fr.avg_days_on_hand_at_year_end > 365*4 THEN 'Critical: Obsolete'
            WHEN fr.avg_days_on_hand_at_year_end > 365*2 THEN 'Warning: Slow Moving'
            WHEN fr.ending_net_realizable_value < (fr.ending_gross_inv_value - 0.01) THEN 'Adjustment Required: NRV < Cost'
            ELSE 'Healthy'
        END AS inventory_risk_rating,

        -- 4. Audit Metadata (from audit_materiality_thresholds seed)
        mat.risk_tier,
        mat.materiality_threshold

    FROM final_report fr
    LEFT JOIN {{ ref('audit_materiality_thresholds') }} mat
        ON fr.product_category = mat.product_category
)

SELECT * FROM final
WHERE period_purchase_amount > 0 OR period_cogs_amount > 0 OR ending_inv_qty > 0
ORDER BY product_id, fiscal_year