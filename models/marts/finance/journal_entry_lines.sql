{{
    config(
        tags = ['financial', 'audit_ready']
    )
}}

-- Subledger support for every journal entry: one row per source document
-- (order for revenue, order item for returns, inventory item for COGS).
-- Amounts come straight from the marts that own each recognition rule, so
-- the GL never re-derives accounting logic.

with revenue as (
    select * from {{ ref('revenue') }}
),

order_item_revenue as (
    select * from {{ ref('order_item_revenue') }}
),

int_inventory_items_joined as (
    select * from {{ ref('int_inventory_items_joined') }}
),

-- Literals are cast explicitly: Athena's Iceberg tables reject the bounded
-- varchar(n) type Trino would otherwise infer for them.
revenue_lines as (
    select
        cast('REV' as {{ dbt.type_string() }}) as entry_type,
        order_id as source_ref,
        cast(shipped_at as date) as posting_date,
        recognized_revenue as amount
    from revenue
    where shipped_at is not null and recognized_revenue <> 0
),

return_lines as (
    select
        cast('RET' as {{ dbt.type_string() }}) as entry_type,
        order_item_id as source_ref,
        cast(returned_at as date) as posting_date,
        sale_price as amount
    from order_item_revenue
    where order_item_status = 'returned' and returned_at is not null
),

-- Same population inventory_fiscal_report sums into period COGS.
cogs_lines as (
    select
        cast('COGS' as {{ dbt.type_string() }}) as entry_type,
        inventory_item_id as source_ref,
        cast(outbound_at as date) as posting_date,
        historical_unit_cost as amount
    from int_inventory_items_joined
    where stock_status = 'Sold (COGS)'
),

unioned as (
    select * from revenue_lines
    union all
    select * from return_lines
    union all
    select * from cogs_lines
),

final as (
    select
        'JE-' || cast(posting_date as {{ dbt.type_string() }}) || '-' || entry_type
            as je_id,
        entry_type,
        posting_date,
        source_ref,
        round(amount, 2) as amount
    from unioned
)

select * from final
