WITH source AS (
    SELECT * FROM {{source('thelook_ecommerce', 'raw_orders')}}
),
reformat AS (
    SELECT
        CAST(order_id as string) AS order_id, -- make key value integer -> string
        CAST(user_id as string) AS user_id,
        LOWER(status) AS order_status, -- to have consistent lower character
        LOWER(gender) AS user_gender,
        created_at, -- UTC
        returned_at, 
        shipped_at,
        delivered_at,
        num_of_item
    FROM
        source
)
SELECT * FROM reformat