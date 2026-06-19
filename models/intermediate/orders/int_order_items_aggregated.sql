WITH source_order_items AS (
    SELECT * FROM {{ ref('stg_thelook_ecommerce__order_items') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_incremental__order_items') }}
),

aggregate_items_to_order_grain AS (
    SELECT
        order_id,
        user_id,
        STRING_AGG(DISTINCT order_item_status ORDER BY order_item_status) AS order_item_status_list,
        COUNT(*)                                                           AS subledger_item_count,
        ROUND(SUM(sale_price), 2)                                         AS total_order_amount,
        MIN(created_at)                                                    AS first_item_created_at,
        COUNT(CASE WHEN order_item_status = 'returned' THEN 1 END)        AS returned_item_count,
        ROUND(SUM(CASE WHEN order_item_status = 'returned'
                       THEN sale_price ELSE 0 END), 2)                    AS refund_amount,
        MAX(returned_at)                                                   AS last_refund_at
    FROM source_order_items
    GROUP BY order_id, user_id
)

SELECT * FROM aggregate_items_to_order_grain
