{% snapshot scd_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['cost', 'retail_price', 'product_name', 'category'],
        invalidate_hard_deletes=True
    )
}}

select
    cast(id as varchar) as product_id,
    cast(cost as double) as cost,
    cast(retail_price as double) as retail_price,
    lower(category) as category,
    lower(brand) as brand,
    name as product_name
from {{ source('thelook_ecommerce', 'raw_products') }}
{% endsnapshot %}