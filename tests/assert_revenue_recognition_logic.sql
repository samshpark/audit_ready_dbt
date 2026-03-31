SELECT
    order_id,
    shipped_at,
    recognized_revenue,
    gross_revenue
FROM {{ ref('fct_revenue') }}
WHERE (shipped_at IS NULL AND recognized_revenue != 0)
   OR (shipped_at IS NOT NULL AND recognized_revenue = 0 AND gross_revenue > 0)