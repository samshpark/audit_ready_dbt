with inventory_items as (
    select * from {{ ref('int_inventory_items_unioned') }}
)

select
    inventory_item_id,
    product_id,
    product_category,
    product_brand,
    product_name,
    cost,
    product_retail_price,
    created_at,
    sold_at,
    (sold_at is null) as is_unsold,
    extract(year from cast(created_at as timestamp)) as received_fiscal_year
from inventory_items
