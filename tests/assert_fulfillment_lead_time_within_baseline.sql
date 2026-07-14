{{ config(severity='warn') }}

-- Known source-data defect (see audit_fulfillment_lead_time_anomalies), not
-- a build-blocking bug — warns only if the rate climbs well past its
-- historical ~5-10% baseline.

with items as (
    select *
    from {{ ref('order_item_revenue') }}
    where shipped_at is not null
),

agg as (
    select
        count(*) as total_shipped,
        sum(case when shipped_at < created_at then 1 else 0 end)
            as negative_lead_time_count,
        round(
            sum(case when shipped_at < created_at then 1 else 0 end)::float
            / nullif(count(*), 0),
            4
        ) as negative_lead_time_rate
    from items
)

select *
from agg
where negative_lead_time_rate > 0.15
