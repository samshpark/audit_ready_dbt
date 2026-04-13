WITH source AS (
    SELECT * FROM {{source('thelook_ecommerce', 'raw_orders')}}
),
reformat AS (
    SELECT
        CAST(order_id as string) AS order_id, -- make key value integer -> string
        CAST(user_id as string) AS user_id,
        LOWER(status) AS order_status, -- to have consistent lower character
        LOWER(gender) AS user_gender,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(returned_at AS TIMESTAMP) AS returned_at,
        CAST(shipped_at AS TIMESTAMP) AS shipped_at,
        CAST(delivered_at AS TIMESTAMP)AS delivered_at,
        num_of_item
    FROM
        source
)
SELECT * FROM reformat