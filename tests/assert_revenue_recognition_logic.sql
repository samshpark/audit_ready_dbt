select
    order_id,
    shipped_at,
    recognized_revenue,
    gross_revenue
from {{ ref('revenue') }}
where
    (shipped_at is null and recognized_revenue <> 0)
    or (shipped_at is not null and recognized_revenue = 0 and gross_revenue > 0)
