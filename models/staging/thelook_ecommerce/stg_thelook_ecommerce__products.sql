with source as (
    select * from {{ source('thelook_ecommerce', 'raw_products') }}
),

renamed as (
    select
        cast(id as {{ dbt.type_string() }}) as product_id,
        cast(distribution_center_id as {{ dbt.type_string() }}) as distribution_center_id,
        lower(category) as category,
        name,
        brand,
        department,
        sku,
        cast(cost as {{ dbt.type_float() }}) as cost,
        cast(retail_price as {{ dbt.type_float() }}) as retail_price
    from
        source
)

select * from renamed
