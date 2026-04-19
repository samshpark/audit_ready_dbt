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

SELECT
    CAST(id AS VARCHAR)              AS product_id,
    CAST(cost AS DOUBLE)             AS cost,
    CAST(retail_price AS DOUBLE)     AS retail_price,
    LOWER(category)                  AS category,
    LOWER(brand)                     AS brand,
    name                             AS product_name
FROM {{ source('thelook_ecommerce', 'raw_products') }}

{% endsnapshot %}

SELECT * FROM snapshots.scd_products LIMIT 10;