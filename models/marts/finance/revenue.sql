{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns'
    )
}}

with int_orders_joined as (
    select * from {{ ref('int_orders_joined') }}
),

final as (
    select
        order_id,
        user_id,
        created_at,
        shipped_at,
        total_order_amount as gross_revenue,
        coalesce(subledger_order_status, master_order_status) as order_status,
        case
            when shipped_at is not null then total_order_amount
            else 0
        end as recognized_revenue,
        coalesce(subledger_item_count, master_item_count) as total_item_count,
        case
            when master_order_id is null then 'ERR: ORPHAN SUB-LEDGER'
            when subledger_order_id is null then 'ERR: MISSING SUB-LEDGER'
            else 'MATCHED'
        end as data_integrity_status,
        case
            when shipped_at is null then 'SHIPPING_PENDING'
            when
                date_trunc('month', cast(created_at as timestamp))
                <> date_trunc('month', cast(shipped_at as timestamp))
                then 'POTENTIAL CUT-OFF RISK'
            else 'NORMAL'
        end as cutoff_status
    from int_orders_joined
)

select * from final
{% if is_incremental() %}
    where order_id in (
        select int_orders_joined.order_id
        from int_orders_joined
        where
            int_orders_joined.first_item_created_at
            >= current_timestamp
            - interval '{{ var("incremental_lookback_days") }} days'
            or int_orders_joined.last_refund_at
            >= current_timestamp
            - interval '{{ var("incremental_lookback_days") }} days'
            or int_orders_joined.shipped_at
            >= current_timestamp
            - interval '{{ var("incremental_lookback_days") }} days'
    )
{% endif %}
