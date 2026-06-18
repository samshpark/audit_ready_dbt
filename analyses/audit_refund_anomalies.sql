-- Flags high-risk refund patterns for revenue integrity review:
--   1. Partial refunds: mixed-status orders (the edge case this project was built to detect)
--   2. High-value refunds: refund amount exceeds 50% of gross order value
--   3. Full refunds on large orders: 100% reversal of significant revenue
--
-- Usage: dbt compile -s analyses/audit_refund_anomalies
--        then copy SQL from target/compiled/ and run directly

WITH refunds AS (
    SELECT * FROM {{ ref('refund_reconciliation') }}
),

anomalies AS (
    SELECT
        order_id,
        user_id,
        order_date,
        gross_revenue,
        refund_amount,
        net_revenue,
        total_item_count,
        total_returned_items,
        refund_value_rate,
        refund_count_rate,
        refund_type,

        CASE
            WHEN refund_type = 'PARTIALLY REFUNDED'          THEN 'PARTIAL REFUND - MIXED STATUS ORDER'
            WHEN refund_type = 'FULLY REFUNDED'
             AND gross_revenue >= 100                         THEN 'FULL REFUND - HIGH VALUE ORDER'
            WHEN refund_value_rate > 0.5
             AND refund_type = 'PARTIALLY REFUNDED'          THEN 'PARTIAL REFUND - EXCEEDS 50% OF ORDER VALUE'
        END AS anomaly_flag

    FROM refunds
    WHERE refund_type != 'NO REFUND'
),

flagged AS (
    SELECT * FROM anomalies
    WHERE anomaly_flag IS NOT NULL
)

SELECT *
FROM flagged
ORDER BY
    CASE anomaly_flag
        WHEN 'FULL REFUND - HIGH VALUE ORDER'            THEN 1
        WHEN 'PARTIAL REFUND - EXCEEDS 50% OF ORDER VALUE' THEN 2
        WHEN 'PARTIAL REFUND - MIXED STATUS ORDER'       THEN 3
    END,
    gross_revenue DESC
