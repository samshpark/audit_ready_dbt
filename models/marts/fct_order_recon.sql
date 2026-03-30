-- Save a result as table
{{ config(materialized = 'table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT
        order_id,
        COUNT(*) AS num_of_item,
        ROUND(SUM(sale_price), 2) AS tot_order_amt,
        STRING_AGG(DISTINCT order_item_status) AS order_status
    FROM
        {{ ref('stg_order_items') }}
    GROUP BY
        1
),
final AS (

    SELECT
        COALESCE(o.order_id, oi.order_id) AS order_id,
        o.user_id,
        o.created_at AS order_at, 
        oi.tot_order_amt, -- subledger amount, orders table does not have amount.

        -- Comparison data
        o.num_of_item AS master_item_count,
        oi.num_of_item AS subledger_item_count,

        -- 1. Existence Reconciliation
        CASE 
            WHEN o.order_id IS NULL THEN 'ERR: ORPHAN SUB-LEDGER'
            WHEN oi.order_id IS NULL THEN 'ERR: MISSING SUB-LEDGER'
            ELSE 'RECONCILIATION SUCCESSFUL' 
        END AS order_reconciliation,

        -- 2. Item Count Reconciliation
        CASE 
            WHEN o.order_id IS NOT NULL AND oi.order_id IS NOT NULL THEN
                CASE 
                    WHEN COALESCE(o.num_of_item, 0) = COALESCE(oi.num_of_item, 0) THEN 'INTEGRITY VERIFIED'
                    ELSE 'VARIANCE DETECTED'
                END
            ELSE 'N/A - PREVIOUS ERROR'
        END AS order_item_count_recon,
        
        -- 3. Order Status Reconciliation
        CASE
            WHEN o.order_id IS NOT NULL AND oi.order_id IS NOT NULL THEN
                CASE 
                    WHEN o.order_status = oi.order_status THEN 'INTEGRITY VERIFIED'
                    ELSE 'VARIANCE DETECTED'
                END
            ELSE 'N/A - PREVIOUS ERROR'
        END AS order_status_recon
    FROM orders o
    FULL JOIN order_items oi
        ON o.order_id = oi.order_id
)

SELECT * FROM final
