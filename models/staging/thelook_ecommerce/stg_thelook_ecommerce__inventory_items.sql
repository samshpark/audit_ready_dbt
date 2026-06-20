with source as (
    select * from {{ source('thelook_ecommerce', 'raw_inventory_items') }}
),

renamed as (
    select
        cast(id as string) as inventory_item_id,
        cast(product_id as string) as product_id,
        product_distribution_center_id,
        lower(product_category) as product_category,
        product_name,
        product_brand,
        lower(product_department) as product_department,
        product_sku,
        cast(cost as double) as cost,
        cast(product_retail_price as double) as product_retail_price,
        cast(created_at as timestamp) as created_at,
        cast(sold_at as timestamp) as sold_at
    from
        source
)

select * from renamed
