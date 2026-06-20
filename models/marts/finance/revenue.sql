{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

WITH base AS (
    SELECT * FROM {{ ref('int_orders_joined') }}
),

final AS (
    SELECT
        order_id,
        user_id,
        created_at,
        shipped_at,
        total_order_amount AS gross_revenue,
        COALESCE(subledger_order_status, master_order_status) AS order_status,
        CASE
            WHEN shipped_at IS NOT NULL THEN total_order_amount
            ELSE 0
        END AS recognized_revenue,
        COALESCE(subledger_item_count, master_item_count) AS total_item_count,
        CASE
            WHEN master_order_id IS NULL THEN 'ERR: ORPHAN SUB-LEDGER'
            WHEN subledger_order_id IS NULL THEN 'ERR: MISSING SUB-LEDGER'
            ELSE 'MATCHED'
        END AS data_integrity_status,
        CASE
            WHEN shipped_at IS NULL THEN 'SHIPPING_PENDING'
            WHEN
                DATE_TRUNC('month', CAST(created_at AS TIMESTAMP))
                != DATE_TRUNC('month', CAST(shipped_at AS TIMESTAMP))
                THEN 'POTENTIAL CUT-OFF RISK'
            ELSE 'NORMAL'
        END AS cutoff_status
    FROM base
)

SELECT * FROM final
{% if is_incremental() %}
    WHERE order_id IN (
        SELECT ij.order_id
        FROM {{ ref('int_orders_joined') }} AS ij
        WHERE
            ij.first_item_created_at
            >= CURRENT_TIMESTAMP
            - INTERVAL '{{ var("incremental_lookback_days") }} days'
            OR ij.last_refund_at
            >= CURRENT_TIMESTAMP
            - INTERVAL '{{ var("incremental_lookback_days") }} days'
    )
{% endif %}
