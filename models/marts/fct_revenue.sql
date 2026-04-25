{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT
        order_id,
        user_id,
        order_item_status_agg AS order_status,
        subledger_item_count AS num_of_item,
        tot_order_amt
    FROM
        {{ ref('int_order_items_summary') }}
),

final AS (
    SELECT
        COALESCE(oi.order_id, o.order_id) AS order_id,
        COALESCE(oi.user_id, o.user_id) AS user_id,
        COALESCE(oi.order_status, o.order_status) AS order_status,
        o.created_at,
        o.shipped_at,
        CASE
            WHEN o.shipped_at IS NOT NULL THEN oi.tot_order_amt
            ELSE 0
        END AS recognized_revenue,
        oi.tot_order_amt AS gross_revenue,
        COALESCE(oi.num_of_item, o.num_of_item) AS total_item_count,
        CASE
            WHEN o.order_id IS NULL THEN 'ERR: ORPHAN SUB-LEDGER'
            WHEN oi.order_id IS NULL THEN 'ERR: MISSING SUB-LEDGER'
            ELSE 'MATCHED'
        END as data_integrity_status,
        CASE
            WHEN o.shipped_at IS NULL THEN 'SHIPPING_PENDING'
            WHEN date_trunc('month', CAST(o.created_at AS TIMESTAMP)) != date_trunc('month', CAST(o.shipped_at AS TIMESTAMP))
                THEN 'POTENTIAL CUT-OFF RISK'
            ELSE 'NORMAL'
        END as cutoff_status
    FROM order_items oi
    FULL JOIN orders o ON oi.order_id = o.order_id
)

SELECT * FROM final
{% if is_incremental() %}
-- Only process orders touched by the most recent int_order_items_summary incremental run
WHERE order_id IN (
    SELECT order_id FROM {{ ref('int_order_items_summary') }}
    WHERE dbt_updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
)
{% endif %}