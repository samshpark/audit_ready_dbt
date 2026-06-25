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
        created_at as order_at,
        total_order_amount,

        {# Comparison data #}

        master_item_count,
        subledger_item_count,

        {# 1. Existence Reconciliation #}

        case
            when master_order_id is null then 'ERR: ORPHAN SUB-LEDGER'
            when subledger_order_id is null then 'ERR: MISSING SUB-LEDGER'
            else 'RECONCILIATION SUCCESSFUL'
        end as order_reconciliation,

        {# 2. Item Count Reconciliation #}

        case
            when master_order_id is not null and subledger_order_id is not null
                then
                    case
                        when
                            coalesce(master_item_count, 0)
                            = coalesce(subledger_item_count, 0)
                            then 'INTEGRITY VERIFIED'
                        else 'VARIANCE DETECTED'
                    end
            else 'N/A - PREVIOUS ERROR'
        end as order_item_count_recon,

        {# 3. Order Status Reconciliation #}

        case
            when master_order_id is not null and subledger_order_id is not null
                then
                    case
                        when
                            master_order_status = subledger_order_status
                            then 'INTEGRITY VERIFIED'
                        else 'VARIANCE DETECTED'
                    end
            else 'N/A - PREVIOUS ERROR'
        end as order_status_recon
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
    )
{% endif %}
