    -- Save as result as table
    {{ config(materialized = 'table') }}

    WITH orders AS (
        SELECT * FROM {{ ref('stg_orders') }}
    ),

    order_items AS (
        SELECT
            order_id,
            user_id,
            STRING_AGG(DISTINCT order_item_status) AS order_status,
            COUNT(*) AS num_of_item,
            ROUND(SUM(sale_price), 2) AS tot_order_amt
        FROM
            {{ ref('stg_order_items') }}
        GROUP BY
            1, 2
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