with orders as (
    select * from {{ ref('stg_thelook_ecommerce__orders') }}
    union all
    select * from {{ ref('stg_incremental__orders') }}
),

int_order_items_aggregated as (
    select * from {{ ref('int_order_items_aggregated') }}
),

final as (
    select
        coalesce(order_items.order_id, orders.order_id) as order_id,
        coalesce(order_items.user_id, orders.user_id) as user_id,

        {# Master ledger columns (stg_orders) #}

        orders.order_id as master_order_id,
        orders.order_status as master_order_status,
        orders.number_of_items as master_item_count,
        orders.created_at,
        orders.shipped_at,
        orders.returned_at,
        orders.delivered_at,

        {# Subledger columns (int_order_items_aggregated) #}

        order_items.order_id as subledger_order_id,
        order_items.order_item_status_list as subledger_order_status,
        order_items.subledger_item_count,
        order_items.total_order_amount,
        order_items.first_item_created_at,
        order_items.returned_item_count,
        order_items.refund_amount,
        order_items.last_refund_at

    from int_order_items_aggregated as order_items
    full join orders on order_items.order_id = orders.order_id
)

select * from final
