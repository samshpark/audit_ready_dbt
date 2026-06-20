with source as (
    select * from {{ source('incremental', 'incr_orders') }}
),

renamed as (
    select
        cast(order_id as string) as order_id,
        cast(user_id as string) as user_id,
        lower(status) as order_status,
        lower(gender) as user_gender,
        num_of_item as number_of_items,
        cast(created_at as timestamp) as created_at,
        cast(returned_at as timestamp) as returned_at,
        cast(shipped_at as timestamp) as shipped_at,
        cast(delivered_at as timestamp) as delivered_at
    from source
)

select * from renamed
