-- Identifies revenue recognition exceptions for period-end audit procedures:
--   1. Cut-off risk: orders created in one month but shipped in another
--   2. Unrecognized revenue: shipped but not recognized (data integrity gap)
--   3. Pending shipments: orders with gross revenue but $0 recognized
--
-- Usage: dbt compile -s analyses/audit_revenue_cutoff_risk
--        then copy SQL from target/compiled/ and run directly

with revenue as (
    select * from {{ ref('revenue') }}
),

exceptions as (
    select
        order_id,
        user_id,
        order_status,
        created_at,
        shipped_at,
        gross_revenue,
        recognized_revenue,
        gross_revenue - recognized_revenue as unrecognized_amount,
        data_integrity_status,
        cutoff_status,

        -- Month boundary detection
        date_trunc('month', cast(created_at as date)) as order_month,
        date_trunc('month', cast(shipped_at as date)) as ship_month

    from revenue
    where
        cutoff_status <> 'NORMAL'
        or data_integrity_status <> 'MATCHED'
)

select *
from exceptions
order by
    case cutoff_status
        when 'POTENTIAL CUT-OFF RISK' then 1
        when 'SHIPPING_PENDING' then 2
        else 3
    end,
    gross_revenue desc
