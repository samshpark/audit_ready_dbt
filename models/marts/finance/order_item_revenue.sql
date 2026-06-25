{{
    config(
        materialized = 'incremental',
        unique_key = 'order_item_id',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns'
    )
}}

with order_items as (
    select * from {{ ref('int_order_items_unioned') }}
),

final as (
    select
        order_item_id,
        order_id,
        user_id,
        product_id,
        order_item_status,
        sale_price,
        case
            when shipped_at is not null then sale_price
            else 0
        end as recognized_item_revenue,
        created_at,
        shipped_at,
        returned_at
    from order_items
)

select * from final
{% if is_incremental() %}
    where
        created_at >= current_timestamp - interval '{{ var("incremental_lookback_days") }} days'
        or shipped_at >= current_timestamp - interval '{{ var("incremental_lookback_days") }} days'
        or returned_at >= current_timestamp - interval '{{ var("incremental_lookback_days") }} days'
{% endif %}
