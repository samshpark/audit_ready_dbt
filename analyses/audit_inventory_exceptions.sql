-- Identifies inventory records that require auditor attention:
--   1. Inventory equation imbalance (Beginning + Purchase - Ending != COGS)
--   2. LCM write-down required (NRV < Cost)
--   3. Slow-moving or obsolete stock
--
-- Usage: dbt compile -s analyses/audit_inventory_exceptions
--        then copy SQL from target/compiled/ and run directly

with inventory as (
    select * from {{ ref('inventory_fiscal_report') }}
),

flagged as (
    select
        fiscal_year,
        product_id,
        product_name,
        product_category,
        product_brand,
        audit_check_diff,
        ending_gross_inv_value,
        ending_net_realizable_value,
        ending_allowance_lcm,
        inventory_risk_rating,
        avg_days_on_hand_at_year_end,
        inventory_turnover_ratio,
        gross_profit_margin,

        case
            when audit_check_diff <> 0
                then 'INVENTORY EQUATION IMBALANCE'
            when inventory_risk_rating = 'Critical: Obsolete'
                then 'OBSOLETE STOCK'
            when inventory_risk_rating = 'Warning: Slow Moving'
                then 'SLOW-MOVING STOCK'
            when inventory_risk_rating = 'Adjustment Required: NRV < Cost'
                then 'LCM WRITE-DOWN REQUIRED'
        end as exception_type

    from inventory
    where
        audit_check_diff <> 0
        or inventory_risk_rating <> 'Healthy'
)

select *
from flagged
order by
    case exception_type
        when 'INVENTORY EQUATION IMBALANCE' then 1
        when 'LCM WRITE-DOWN REQUIRED' then 2
        when 'OBSOLETE STOCK' then 3
        when 'SLOW-MOVING STOCK' then 4
    end,
    abs(audit_check_diff) desc
