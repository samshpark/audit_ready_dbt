with source as (
    select * from {{ source('thelook_ecommerce', 'raw_order_items') }}
),

renamed as (
    select
        cast(id as string) as order_item_id,
        cast(order_id as string) as order_id,
        cast(user_id as string) as user_id,
        cast(product_id as string) as product_id,
        cast(inventory_item_id as string) as inventory_item_id,
        lower(status) as order_item_status,
        cast(sale_price as double) as sale_price,
        cast(created_at as timestamp) as created_at,
        cast(shipped_at as timestamp) as shipped_at,
        cast(delivered_at as timestamp) as delivered_at,
        cast(returned_at as timestamp) as returned_at
    from
        source
)

select * from renamed
