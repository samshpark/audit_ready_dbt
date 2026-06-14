{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_thelook_ecommerce__orders') }}
),

order_items AS (
    SELECT
        order_id,
        subledger_item_count AS num_of_item,
        tot_order_amt,
        order_item_status_agg AS order_status
    FROM
        {{ ref('int_order_items_summary') }}
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
{% if is_incremental() %}
-- Only process orders touched by the most recent int_order_items_summary incremental run
WHERE order_id IN (
    SELECT order_id FROM {{ ref('int_order_items_summary') }}
    WHERE dbt_updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
)
{% endif %}
