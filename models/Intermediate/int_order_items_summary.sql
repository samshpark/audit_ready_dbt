{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    -- Lookback window: re-process any order touched in the last 7 days.
    -- Subquery pulls ALL items for those orders so aggregates stay correct.
    -- returned_at filter catches refunds that arrive days/weeks after the original order.
    WHERE order_id IN (
        SELECT DISTINCT order_id
        FROM {{ ref('stg_order_items') }}
        WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
           OR returned_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
    )
    {% endif %}
),

final AS (
    SELECT
        order_id,
        user_id,
        STRING_AGG(DISTINCT order_item_status ORDER BY order_item_status) AS order_item_status_agg,
        COUNT(*)                                                           AS subledger_item_count,
        ROUND(SUM(sale_price), 2)                                         AS tot_order_amt,
        MIN(created_at)                                                    AS first_item_created_at,
        COUNT(CASE WHEN order_item_status = 'returned' THEN 1 END)        AS returned_item_count,
        ROUND(SUM(CASE WHEN order_item_status = 'returned'
                       THEN sale_price ELSE 0 END), 2)                    AS refund_amount,
        MAX(returned_at)                                                   AS last_refund_at,
        CURRENT_TIMESTAMP                                                  AS dbt_updated_at
    FROM order_items
    GROUP BY 1, 2
)

SELECT * FROM final
