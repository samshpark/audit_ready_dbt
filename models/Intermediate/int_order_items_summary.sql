WITH order_items AS (
      SELECT * FROM {{ ref('stg_order_items') }}
  ),

  final AS (
      SELECT
          order_id,
          user_id,
          STRING_AGG(DISTINCT order_item_status ORDER BY order_item_status) AS order_item_status_agg,
          COUNT(*)                              AS subledger_item_count,
          ROUND(SUM(sale_price), 2)             AS tot_order_amt,
          MIN(created_at)                       AS first_item_created_at,
          COUNT(CASE WHEN order_item_status = 'returned' THEN 1 END) AS returned_item_count,
          ROUND(SUM(CASE WHEN order_item_status = 'returned' THEN sale_price ELSE 0 END), 2) AS refund_amount
      FROM order_items
      GROUP BY 1, 2
  )

  SELECT * FROM final
