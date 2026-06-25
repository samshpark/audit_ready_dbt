with thelook as (
    select * from {{ ref('stg_thelook_ecommerce__order_items') }}
),

incremental as (
    select * from {{ ref('stg_incremental__order_items') }}
),

unioned as (
    select * from thelook
    union all
    select * from incremental
)

select * from unioned
