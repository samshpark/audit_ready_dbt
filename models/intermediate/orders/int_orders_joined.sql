WITH orders AS (
    SELECT * FROM {{ ref('stg_thelook_ecommerce__orders') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_incremental__orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('int_order_items_aggregated') }}
),

final AS (
    SELECT
        COALESCE(oi.order_id, o.order_id)   AS order_id,
        COALESCE(oi.user_id, o.user_id)     AS user_id,

        -- Master ledger columns (stg_orders)
        o.order_id                           AS master_order_id,
        o.order_status                       AS master_order_status,
        o.num_of_item                        AS master_item_count,
        o.created_at,
        o.shipped_at,
        o.returned_at,
        o.delivered_at,

        -- Subledger columns (int_order_items_aggregated)
        oi.order_id                          AS subledger_order_id,
        oi.order_item_status_agg             AS subledger_order_status,
        oi.subledger_item_count,
        oi.total_order_amount,
        oi.first_item_created_at,
        oi.returned_item_count,
        oi.refund_amount,
        oi.last_refund_at

    FROM order_items oi
    FULL JOIN orders o ON oi.order_id = o.order_id
)

SELECT * FROM final
