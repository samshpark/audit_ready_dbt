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

products as (
    select * from {{ ref('stg_thelook_ecommerce__products') }}
),

final as (
    select
        order_items.order_item_id,
        order_items.order_id,
        order_items.user_id,
        order_items.product_id,
        products.category as product_category,
        products.brand as product_brand,
        products.name as product_name,
        order_items.order_item_status,
        order_items.sale_price,
        case
            when order_items.shipped_at is not null then order_items.sale_price
            else 0
        end as recognized_item_revenue,
        order_items.created_at,
        order_items.shipped_at,
        order_items.returned_at
    from order_items
    left join products on order_items.product_id = products.product_id
)

select * from final
{% if is_incremental() %}
    where
        created_at >= current_timestamp - interval '{{ var("incremental_lookback_days") }} days'
        or shipped_at >= current_timestamp - interval '{{ var("incremental_lookback_days") }} days'
        or returned_at >= current_timestamp - interval '{{ var("incremental_lookback_days") }} days'
{% endif %}
