with inbound as (
    select
        inventory_item_id,
        product_id,
        product_category,
        product_name,
        product_brand,
        created_at as inbound_at,
        cost,
        product_retail_price,
        product_department
    from {{ ref('stg_thelook_ecommerce__inventory_items') }}
    union all
    select
        inventory_item_id,
        product_id,
        product_category,
        product_name,
        product_brand,
        created_at as inbound_at,
        cost,
        product_retail_price,
        product_department
    from {{ ref('stg_incremental__inventory_items') }}
),

outbound as (
    select
        inventory_item_id,
        product_id,
        shipped_at as outbound_at,
        sale_price
    from {{ ref('stg_thelook_ecommerce__order_items') }}
    where order_item_status not in ('cancelled', 'returned', 'processing')
    union all
    select
        inventory_item_id,
        product_id,
        shipped_at as outbound_at,
        sale_price
    from {{ ref('stg_incremental__order_items') }}
    where order_item_status not in ('cancelled', 'returned', 'processing')
),

join_inbound_to_outbound as (
    select
        inbound.product_id as inbound_product_id,
        outbound.product_id as outbound_product_id,
        inbound.product_category,
        inbound.product_name,
        inbound.product_brand,
        outbound.outbound_at,
        inbound.cost as historical_unit_cost,

        {# Guard against outbound-before-inbound records #}
        {# (breaks days_in_inventory and P&L audit checks) #}

        inbound.product_retail_price as current_market_price,

        outbound.sale_price,
        coalesce(inbound.inventory_item_id, outbound.inventory_item_id)
            as inventory_item_id,
        coalesce(inbound.product_id, outbound.product_id) as product_id,
        case
            when
                outbound.outbound_at is not null
                and inbound.inbound_at > outbound.outbound_at
                then outbound.outbound_at
            else inbound.inbound_at
        end as inbound_at
    from inbound
    full join outbound
        on inbound.inventory_item_id = outbound.inventory_item_id
),

calculate_inventory_metrics as (
    select
        inventory_item_id,
        product_id,
        product_category,
        product_name,
        product_brand,
        inbound_at,
        outbound_at,
        historical_unit_cost,
        current_market_price,
        sale_price,
        {# Evaluate LCM (Lower of Cost or Market) #}

        least(historical_unit_cost, current_market_price) as lcm_unit_valuation,
        {# "Allowance for Inventory Valuation" in Financial Statements #}

        historical_unit_cost
        - least(historical_unit_cost, current_market_price)
            as inventory_valuation_loss,

        case
            when inbound_at is null then null
            when outbound_at is not null
                then {{ datediff_days('inbound_at', 'outbound_at') }}
            {# 0 if future inbound #}

            else greatest({{ datediff_days('inbound_at', 'CURRENT_DATE') }}, 0)
        end as days_in_inventory,

        case
            {# 1. Ending Inventory #}

            when outbound_at is null
                then
                    case
                        when
                            days_in_inventory > 365 * 4
                            then 'On-hand: 04. Obsolete (>4yr)'
                        when
                            days_in_inventory > 365 * 3
                            then 'On-hand: 03. Slow-moving (3yr-4yr)'
                        when
                            days_in_inventory > 365 * 2
                            then 'On-hand: 02. Stagnant (2yr-3yr)'
                        else 'On-hand: 01. Healthy (<2y)'
                    end

            {# 2. COGs #}

            when
                days_in_inventory > 365 * 4
                then 'Sold: 04. Very Slow (>4yr)'
            when
                days_in_inventory > 365 * 3
                then 'Sold: 03. Slow (3yr-4yr)'
            when
                days_in_inventory > 365 * 2
                then 'Sold: 02. Normal (2yr-3yr)'
            else 'Sold: 01. Fast (<2y)'
        end as aging_velocity_bucket,

        case
            when
                inventory_item_id is null
                then 'Error: Outbound without Inbound'
            when outbound_at is null then 'On-hand (Ending Inventory)'
            else 'Sold (COGS)'
        end as stock_status,

        {# Evaluate whether the product_id is a match #}

        case
            when
                inbound_product_id is not null
                and outbound_product_id is not null
                and inbound_product_id <> outbound_product_id
                then 'Product Not Match'
            else 'Product Match'
        end as product_match,
        extract(year from cast(outbound_at as timestamp))
            as outbound_fiscal_year,
        extract(year from cast(inbound_at as timestamp)) as inbound_fiscal_year

    from join_inbound_to_outbound
)

select * from calculate_inventory_metrics
