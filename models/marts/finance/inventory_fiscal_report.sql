{{
    config(
        tags = ['financial', 'audit_ready']
    )
}}

with int_inventory_items_joined as (
    select * from {{ ref('int_inventory_items_joined') }}
),

scd_products as (
    select * from {{ ref('scd_products') }}
),

audit_materiality_thresholds as (
    select * from {{ ref('audit_materiality_thresholds') }}
),

{# 1. get all available fiscal year #}

years as (
    select distinct inbound_fiscal_year as fiscal_year
    from int_inventory_items_joined
    where inbound_fiscal_year is not null
    union
    select distinct outbound_fiscal_year
    from int_inventory_items_joined
    where outbound_fiscal_year is not null
),

{# 2. By Fiscal Year & Product: Detail Metrics #}

product_year_flow as (
    select
        years.fiscal_year,
        inventory.product_id,
        inventory.product_name,
        inventory.product_category,
        inventory.product_brand,

        {# [P/L] Sold for the fiscal_year #}

        count(
            case
                when
                    inventory.outbound_fiscal_year = years.fiscal_year
                    then inventory.inventory_item_id
            end
        ) as period_units_sold,
        round(
            sum(
                case
                    when
                        inventory.outbound_fiscal_year = years.fiscal_year
                        then inventory.historical_unit_cost
                    else 0
                end
            ),
            2
        ) as period_cogs_amount,
        round(
            sum(
                case
                    when
                        inventory.outbound_fiscal_year = years.fiscal_year
                        then inventory.sale_price
                    else 0
                end
            ),
            2
        ) as period_revenue,

        {# [B/S] On hand inventory: as of the fiscal year end #}

        count(case
            when
                inventory.inbound_fiscal_year <= years.fiscal_year
                and (
                    inventory.outbound_fiscal_year is null
                    or inventory.outbound_fiscal_year > years.fiscal_year
                )
                then inventory.inventory_item_id
        end) as ending_inv_qty,

        round(sum(case
            when
                inventory.inbound_fiscal_year <= years.fiscal_year
                and (
                    inventory.outbound_fiscal_year is null
                    or inventory.outbound_fiscal_year > years.fiscal_year
                )
                then inventory.historical_unit_cost
            else 0
        end), 2) as ending_gross_inv_value,

        {# [LCM & NRV]: as of the fiscal year end #}

        {# Uses scd_products snapshot price (fiscal year Dec 31); #}
        {# falls back to stg_inventory_items price if no snapshot exists. #}

        round(sum(case
            when
                inventory.inbound_fiscal_year <= years.fiscal_year
                and (
                    inventory.outbound_fiscal_year is null
                    or inventory.outbound_fiscal_year > years.fiscal_year
                )
                then
                    greatest(
                        0,
                        inventory.historical_unit_cost
                        - least(
                            inventory.historical_unit_cost,
                            coalesce(
                                scd_products.retail_price,
                                inventory.current_market_price
                            )
                        )
                    )
            else 0
        end), 2) as ending_allowance_lcm,

        round(sum(case
            when
                inventory.inbound_fiscal_year <= years.fiscal_year
                and (
                    inventory.outbound_fiscal_year is null
                    or inventory.outbound_fiscal_year > years.fiscal_year
                )
                then
                    least(
                        inventory.historical_unit_cost,
                        coalesce(
                            scd_products.retail_price,
                            inventory.current_market_price
                        )
                    )
            else 0
        end), 2) as ending_net_realizable_value,

        {# [Period Purchase Amount]: for the fiscal year #}

        round(
            sum(
                case
                    when
                        inventory.inbound_fiscal_year = years.fiscal_year
                        then inventory.historical_unit_cost
                    else 0
                end
            ),
            2
        ) as period_purchase_amount,

        {# [Operational: Aging] Avg days in inventory at fiscal year end #}

        round(avg(case
            when
                inventory.inbound_fiscal_year <= years.fiscal_year
                and (
                    inventory.outbound_fiscal_year is null
                    or inventory.outbound_fiscal_year > years.fiscal_year
                )
                then
                    (
                        {{ fiscal_year_end('years.fiscal_year') }}
                        - cast(inventory.inbound_at as date)
                    )
        end), 1) as avg_days_on_hand_at_year_end

    from years
    cross join int_inventory_items_joined as inventory
    left join scd_products
        on
            inventory.product_id = scd_products.product_id
            and {{ fiscal_year_end('years.fiscal_year') }}
            >= scd_products.dbt_valid_from
            and (
                scd_products.dbt_valid_to is null
                or {{ fiscal_year_end('years.fiscal_year') }}
                < scd_products.dbt_valid_to
            )
    group by 1, 2, 3, 4, 5
),

add_beginning_inventory_value as (
    select
        *,
        {# Beginning Inventory = Ending Inventory of Last Fiscal Year #}

        lag(ending_gross_inv_value, 1, 0)
            over (partition by product_id order by fiscal_year)
            as beginning_inv_value
    from product_year_flow
),

final as (
    select
        add_beginning_inventory_value.fiscal_year,
        add_beginning_inventory_value.product_id,
        add_beginning_inventory_value.product_name,
        add_beginning_inventory_value.product_category,
        add_beginning_inventory_value.product_brand,
        add_beginning_inventory_value.period_units_sold,
        add_beginning_inventory_value.period_cogs_amount,
        add_beginning_inventory_value.period_revenue,
        add_beginning_inventory_value.beginning_inv_value,
        add_beginning_inventory_value.period_purchase_amount,
        add_beginning_inventory_value.ending_gross_inv_value,
        add_beginning_inventory_value.ending_inv_qty,
        add_beginning_inventory_value.ending_allowance_lcm,
        add_beginning_inventory_value.ending_net_realizable_value,
        add_beginning_inventory_value.avg_days_on_hand_at_year_end,

        {# 1. Inventory Reconciliation & Audit Check #}

        {# Formula: (Beginning + Purchase - Ending) - COGS #}

        {# This should ideally be 0. Discrepancies indicate: #}

        {#   (+) Positive: Inventory Shrinkage (theft, breakage) #}
        {#   (-) Negative: Ghost Inventory (unrecorded receipts) #}

        audit_materiality_thresholds.risk_tier,

        {# 2. Financial Ratios #}

        audit_materiality_thresholds.materiality_threshold,
        case
            when
                abs(
                    (
                        add_beginning_inventory_value.beginning_inv_value
                        + add_beginning_inventory_value.period_purchase_amount
                        - add_beginning_inventory_value.ending_gross_inv_value
                    )
                    - add_beginning_inventory_value.period_cogs_amount
                )
                <= 0.019
                {# $0.019 tolerance: floating-point rounding error from #}
                {# round(..., 2) accumulated across item-level aggregations. #}
                {# Variance above this threshold flags real discrepancies. #}
                then 0
            else
                round(
                    (
                        add_beginning_inventory_value.beginning_inv_value
                        + add_beginning_inventory_value.period_purchase_amount
                        - add_beginning_inventory_value.ending_gross_inv_value
                    )
                    - add_beginning_inventory_value.period_cogs_amount,
                    2
                )
        end as audit_check_diff,

        {# 3. Risk Assessment #}

        round(
            add_beginning_inventory_value.period_cogs_amount
            / nullif(
                (
                    add_beginning_inventory_value.beginning_inv_value
                    + add_beginning_inventory_value.ending_gross_inv_value
                ) / 2, 0
            ),
            3
        ) as inventory_turnover_ratio,

        {# 4. Audit Metadata (from audit_materiality_thresholds seed) #}

        round(
            (
                add_beginning_inventory_value.period_revenue
                - add_beginning_inventory_value.period_cogs_amount
            )
            / nullif(add_beginning_inventory_value.period_revenue, 0),
            4
        ) as gross_profit_margin,
        case
            when
                add_beginning_inventory_value.avg_days_on_hand_at_year_end
                > 365 * 4
                then 'Critical: Obsolete'
            when
                add_beginning_inventory_value.avg_days_on_hand_at_year_end
                > 365 * 2
                then 'Warning: Slow Moving'
            when
                add_beginning_inventory_value.ending_net_realizable_value
                < (add_beginning_inventory_value.ending_gross_inv_value - 0.01)
                then 'Adjustment Required: NRV < Cost'
            else 'Healthy'
        end as inventory_risk_rating

    from add_beginning_inventory_value
    left join audit_materiality_thresholds
        on
            add_beginning_inventory_value.product_category
            = audit_materiality_thresholds.product_category
    where
        add_beginning_inventory_value.period_purchase_amount > 0
        or add_beginning_inventory_value.period_cogs_amount > 0
        or add_beginning_inventory_value.ending_inv_qty > 0
)

select * from final
