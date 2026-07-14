-- Flags fulfillment timestamp exceptions on order_item_revenue:
--   1. Negative lead time: shipped_at before created_at (impossible sequence)
--   2. Excessive lead time: over 7 days from order to ship
--
-- Negative lead time already exists in the raw thelook_ecommerce data, not
-- introduced by dbt or the generator — a source defect to monitor, not fix.
-- See assert_fulfillment_lead_time_within_baseline.
--
-- Usage: dbt compile -s audit_fulfillment_lead_time_anomalies
--        then copy SQL from target/compiled/ and run directly

with items as (
    select * from {{ ref('order_item_revenue') }}
    where shipped_at is not null
),

exceptions as (
    select
        order_item_id,
        order_id,
        user_id,
        product_id,
        order_item_status,
        sale_price,
        created_at,
        shipped_at,
        datediff('day', created_at, shipped_at) as lead_time_days,

        case
            when shipped_at < created_at
                then 'NEGATIVE LEAD TIME - IMPOSSIBLE SEQUENCE'
            when datediff('day', created_at, shipped_at) > 7
                then 'EXCESSIVE LEAD TIME - OVER 7 DAYS'
        end as anomaly_flag

    from items
),

flagged as (
    select * from exceptions
    where anomaly_flag is not null
)

select *
from flagged
order by
    case anomaly_flag
        when 'NEGATIVE LEAD TIME - IMPOSSIBLE SEQUENCE' then 1
        else 2
    end,
    abs(lead_time_days) desc
