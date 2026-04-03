-- Save a result as table
{{ config(materialized = 'table') }}

WITH reconciled AS (
    SELECT
        order_id,
        user_id,
        -- All created_at for same order are identical.
        MIN(created_at) AS order_date,
        ROUND(SUM(sale_price), 2) AS gross_revenue,
        ROUND(SUM(CASE WHEN order_item_status = 'returned' THEN sale_price ELSE 0 END), 2) AS refund_amount,
        COUNT(*) AS total_item_count,
        SUM(CASE WHEN order_item_status = 'returned' THEN 1 ELSE 0 END) AS total_returned_items
    FROM {{ ref('stg_order_items') }}
    -- FROM {{ ref('synth_stg_order_items') }} - Used for the testing of the sql for partially refunded scenario
    GROUP BY 1, 2
),

final AS (
    SELECT 
        *,
        (gross_revenue - refund_amount) AS net_revenue,
        ROUND(COALESCE(refund_amount / NULLIF(gross_revenue, 0), 0), 4) AS refund_value_rate,
        ROUND(COALESCE(CAST(total_returned_items AS FLOAT) / NULLIF(total_item_count, 0), 0), 4) AS refund_count_rate,
        CASE 
            WHEN total_returned_items = 0 THEN 'NO REFUND'
            WHEN total_returned_items = total_item_count THEN 'FULLY REFUNDED'
            ELSE 'PARTIALLY REFUNDED'
        END AS refund_type
    FROM reconciled
)

SELECT * FROM final
--WHERE refund_type = 'PARTIALLY REFUNDED' : - Used for the testing of the sql for partially refunded scenario