{{ config(
    tags=['financial', 'inventory_valuation', 'audit_ready']
) }}

WITH base_ledger AS (
    SELECT * FROM {{ ref('int_inventory_ledger') }}
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
            THEN (CAST(y.fiscal_year || '-12-31' AS DATE) - CAST(l.inbound_at AS DATE)) -- fiscal year end
        END), 1) AS avg_days_on_hand_at_year_end

    FROM years y
    CROSS JOIN base_ledger l
    LEFT JOIN {{ ref('scd_products') }} scd
        ON  l.product_id = scd.product_id
        AND CAST(y.fiscal_year || '-12-31' AS DATE) >= scd.dbt_valid_from
        AND (scd.dbt_valid_to IS NULL OR CAST(y.fiscal_year || '-12-31' AS DATE) < scd.dbt_valid_to)
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
        fiscal_year,
        product_id,
        product_name,
        product_category,
        product_brand,
        period_units_sold,
        period_cogs_amount,
        period_revenue,
        beginning_inv_value,
        period_purchase_amount,
        ending_gross_inv_value,
        ending_inv_qty, 
        ending_allowance_lcm,
        ending_net_realizable_value,
        avg_days_on_hand_at_year_end,

        -- 1. Inventory Reconciliation & Audit Check
            -- Formula: (Beginning + Purchase - Ending) - COGS
            -- This should ideally be 0. Discrepancies indicate:
                --   (+) Positive: Inventory Shrinkage (Unrecorded loss, theft, or breakage)
                --   (-) Negative: Surplus/Ghost Inventory (Unrecorded receipts or COGS overstatement)
        
        CASE
            WHEN ABS((beginning_inv_value + period_purchase_amount - ending_gross_inv_value) - period_cogs_amount) <= 0.019
            -- Tolerance of $0.019 accounts for floating-point rounding error accumulated from
            -- ROUND(..., 2) applied to individual item-level costs across multiple aggregations.
            -- Any variance exceeding this threshold is a real discrepancy and flagged for investigation.
            THEN 0
            ELSE ROUND((beginning_inv_value + period_purchase_amount - ending_gross_inv_value) - period_cogs_amount, 2)
        END AS audit_check_diff,
        
        -- 2. 재무 비율 (Financial Ratios)
        ROUND(period_cogs_amount / NULLIF((beginning_inv_value + ending_gross_inv_value) / 2, 0), 3) AS inventory_turnover_ratio,
        ROUND((period_revenue - period_cogs_amount) / NULLIF(period_revenue, 0), 4) AS gross_profit_margin,

        -- 3. 리스크 평가 (Risk Assessment)
        CASE 
            WHEN avg_days_on_hand_at_year_end > 365*4 THEN 'Critical: Obsolete'
            WHEN avg_days_on_hand_at_year_end > 365*2 THEN 'Warning: Slow Moving'
            WHEN ending_net_realizable_value < (ending_gross_inv_value - 0.01) THEN 'Adjustment Required: NRV < Cost'
            ELSE 'Healthy'
        END AS inventory_risk_rating

    FROM final_report
)

SELECT * FROM final
WHERE period_purchase_amount > 0 OR period_cogs_amount > 0 OR ending_inv_qty > 0
ORDER BY product_id, fiscal_year