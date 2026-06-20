with source as (
    select * from {{ source('thelook_ecommerce', 'raw_products') }}
),

renamed as (
    select
        cast(id as string) as product_id,
        cast(distribution_center_id as string) as distribution_center_id,
        lower(category) as category,
        name,
        brand,
        department,
        sku,
        cast(cost as double) as cost,
        cast(retail_price as double) as retail_price
    from
        source
)

select * from renamed
