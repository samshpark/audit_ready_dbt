with source_order_items as (
    select * from {{ ref('int_order_items_unioned') }}
),

aggregate_items_to_order_grain as (
    select
        order_id,
        user_id,
        string_agg(distinct order_item_status order by order_item_status)
            as order_item_status_list,
        count(*) as subledger_item_count,
        round(sum(sale_price), 2) as total_order_amount,
        min(created_at) as first_item_created_at,
        count(case when order_item_status = 'returned' then 1 end)
            as returned_item_count,
        round(sum(case
            when order_item_status = 'returned'
                then sale_price
            else 0
        end), 2) as refund_amount,
        max(returned_at) as last_refund_at
    from source_order_items
    group by 1, 2
)

select * from aggregate_items_to_order_grain
