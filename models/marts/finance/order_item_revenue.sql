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
