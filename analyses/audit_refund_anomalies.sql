-- Flags high-risk refund patterns for revenue integrity review:
--   1. Partial refunds: mixed-status orders (edge case this project detects)
--   2. High-value refunds: refund amount exceeds 50% of gross order value
--   3. Full refunds on large orders: 100% reversal of significant revenue
--
-- Usage: dbt compile -s analyses/audit_refund_anomalies
--        then copy SQL from target/compiled/ and run directly

with refunds as (
    select * from {{ ref('refund_reconciliation') }}
),

anomalies as (
    select
        order_id,
        user_id,
        order_date,
        gross_revenue,
        refund_amount,
        net_revenue,
        total_item_count,
        total_returned_items,
        refund_value_rate,
        refund_count_rate,
        refund_type,

        case
            when refund_type = 'PARTIALLY REFUNDED'
                then 'PARTIAL REFUND - MIXED STATUS ORDER'
            when refund_type = 'FULLY REFUNDED' and gross_revenue >= 100
                then 'FULL REFUND - HIGH VALUE ORDER'
            when refund_value_rate > 0.5 and refund_type = 'PARTIALLY REFUNDED'
                then 'PARTIAL REFUND - EXCEEDS 50% OF ORDER VALUE'
        end as anomaly_flag

    from refunds
    where refund_type <> 'NO REFUND'
),

flagged as (
    select * from anomalies
    where anomaly_flag is not null
)

select *
from flagged
order by
    case anomaly_flag
        when 'FULL REFUND - HIGH VALUE ORDER' then 1
        when 'PARTIAL REFUND - EXCEEDS 50% OF ORDER VALUE' then 2
        when 'PARTIAL REFUND - MIXED STATUS ORDER' then 3
    end,
    gross_revenue desc
