-- Identifies inventory records that require auditor attention:
--   1. Inventory equation imbalance (Beginning + Purchase - Ending ≠ COGS)
--   2. LCM write-down required (NRV < Cost)
--   3. Slow-moving or obsolete stock
--
-- Usage: dbt compile -s analyses/audit_inventory_exceptions
--        then copy SQL from target/compiled/ and run directly

WITH inventory AS (
    SELECT * FROM {{ ref('inventory_fiscal_report') }}
),

flagged AS (
    SELECT
        fiscal_year,
        product_id,
        product_name,
        product_category,
        product_brand,
        audit_check_diff,
        ending_gross_inv_value,
        ending_net_realizable_value,
        ending_allowance_lcm,
        inventory_risk_rating,
        avg_days_on_hand_at_year_end,
        inventory_turnover_ratio,
        gross_profit_margin,

        -- Classify the exception type for triage
        CASE
            WHEN audit_check_diff != 0                        THEN 'INVENTORY EQUATION IMBALANCE'
            WHEN inventory_risk_rating = 'Critical: Obsolete' THEN 'OBSOLETE STOCK'
            WHEN inventory_risk_rating = 'Warning: Slow Moving' THEN 'SLOW-MOVING STOCK'
            WHEN inventory_risk_rating = 'Adjustment Required: NRV < Cost' THEN 'LCM WRITE-DOWN REQUIRED'
        END AS exception_type

    FROM inventory
    WHERE
        audit_check_diff != 0
        OR inventory_risk_rating != 'Healthy'
)

SELECT *
FROM flagged
ORDER BY
    CASE exception_type
        WHEN 'INVENTORY EQUATION IMBALANCE'  THEN 1
        WHEN 'LCM WRITE-DOWN REQUIRED'       THEN 2
        WHEN 'OBSOLETE STOCK'                THEN 3
        WHEN 'SLOW-MOVING STOCK'             THEN 4
    END,
    ABS(audit_check_diff) DESC
