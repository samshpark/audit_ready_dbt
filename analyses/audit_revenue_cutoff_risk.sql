-- Identifies revenue recognition exceptions for period-end audit procedures:
--   1. Cut-off risk: orders created in one month but shipped in another
--   2. Unrecognized revenue: shipped but not yet recognized (data integrity gap)
--   3. Pending shipments: orders with gross revenue but $0 recognized
--
-- Usage: dbt compile -s analyses/audit_revenue_cutoff_risk
--        then copy SQL from target/compiled/ and run directly

WITH revenue AS (
    SELECT * FROM {{ ref('revenue') }}
),

exceptions AS (
    SELECT
        order_id,
        user_id,
        order_status,
        created_at,
        shipped_at,
        gross_revenue,
        recognized_revenue,
        gross_revenue - recognized_revenue AS unrecognized_amount,
        data_integrity_status,
        cutoff_status,

        -- Month boundary detection
        DATE_TRUNC('month', CAST(created_at  AS DATE)) AS order_month,
        DATE_TRUNC('month', CAST(shipped_at  AS DATE)) AS ship_month

    FROM revenue
    WHERE cutoff_status != 'NORMAL'
       OR data_integrity_status != 'MATCHED'
)

SELECT *
FROM exceptions
ORDER BY
    CASE cutoff_status
        WHEN 'POTENTIAL CUT-OFF RISK' THEN 1
        WHEN 'SHIPPING_PENDING'       THEN 2
        ELSE 3
    END,
    gross_revenue DESC
