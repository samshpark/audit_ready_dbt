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
        created_at AS order_at,
        total_order_amount,

        -- Comparison data
        master_item_count,
        subledger_item_count,

        -- 1. Existence Reconciliation
        CASE
            WHEN master_order_id IS NULL THEN 'ERR: ORPHAN SUB-LEDGER'
            WHEN subledger_order_id IS NULL THEN 'ERR: MISSING SUB-LEDGER'
            ELSE 'RECONCILIATION SUCCESSFUL'
        END AS order_reconciliation,

        -- 2. Item Count Reconciliation
        CASE
            WHEN master_order_id IS NOT NULL AND subledger_order_id IS NOT NULL THEN
                CASE
                    WHEN COALESCE(master_item_count, 0) = COALESCE(subledger_item_count, 0) THEN 'INTEGRITY VERIFIED'
                    ELSE 'VARIANCE DETECTED'
                END
            ELSE 'N/A - PREVIOUS ERROR'
        END AS order_item_count_recon,

        -- 3. Order Status Reconciliation
        CASE
            WHEN master_order_id IS NOT NULL AND subledger_order_id IS NOT NULL THEN
                CASE
                    WHEN master_order_status = subledger_order_status THEN 'INTEGRITY VERIFIED'
                    ELSE 'VARIANCE DETECTED'
                END
            ELSE 'N/A - PREVIOUS ERROR'
        END AS order_status_recon
    FROM base
)

SELECT * FROM final
{% if is_incremental() %}
WHERE order_id IN (
    SELECT order_id FROM {{ ref('int_orders_joined') }}
    WHERE first_item_created_at >= CURRENT_TIMESTAMP - INTERVAL '{{ var("incremental_lookback_days") }} days'
       OR last_refund_at >= CURRENT_TIMESTAMP - INTERVAL '{{ var("incremental_lookback_days") }} days'
)
{% endif %}
