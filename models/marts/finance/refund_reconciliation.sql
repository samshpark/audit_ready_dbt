{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge'
    )
}}

with int_order_items_aggregated as (
    select * from {{ ref('int_order_items_aggregated') }}
),

select_refund_metrics as (
    select
        order_id,
        user_id,
        first_item_created_at as order_date,
        total_order_amount as gross_revenue,
        refund_amount,
        subledger_item_count as total_item_count,
        returned_item_count as total_returned_items
    from int_order_items_aggregated
    {% if is_incremental() %}
        where
            first_item_created_at
            >= current_timestamp
            - interval '{{ var("incremental_lookback_days") }} days'
            or last_refund_at
            >= current_timestamp
            - interval '{{ var("incremental_lookback_days") }} days'
    {% endif %}
),

final as (
    select
        *,
        (gross_revenue - refund_amount) as net_revenue,
        round(coalesce(refund_amount / nullif(gross_revenue, 0), 0), 4)
            as refund_value_rate,
        round(
            coalesce(
                cast(total_returned_items as float)
                / nullif(total_item_count, 0),
                0
            ),
            4
        ) as refund_count_rate,
        case
            when total_returned_items = 0 then 'NO REFUND'
            when total_returned_items = total_item_count then 'FULLY REFUNDED'
            else 'PARTIALLY REFUNDED'
        end as refund_type
    from select_refund_metrics
)

select * from final
{# WHERE refund_type = 'PARTIALLY REFUNDED' - partial refund test filter #}
