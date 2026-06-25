-- Surfaces all order-level reconciliation failures between the master ledger
-- (raw_orders) and sub-ledger (raw_order_items) for auditor follow-up:
--   1. Orphan sub-ledger: line items exist with no matching order header
--   2. Missing sub-ledger: order header exists with no matching line items
--   3. Item count variance: master and sub-ledger disagree on unit count
--   4. Status variance: order status differs between header and line level
--
-- Usage: dbt compile -s analyses/audit_order_reconciliation_failures
--        then copy SQL from target/compiled/ and run directly

with recon as (
    select * from {{ ref('order_reconciliation') }}
),

failures as (
    select
        order_id,
        user_id,
        order_at,
        total_order_amount,
        master_item_count,
        subledger_item_count,
        master_item_count - subledger_item_count as item_count_variance,
        order_reconciliation as existence_check,
        order_item_count_recon as item_count_check,
        order_status_recon as status_check

    from recon
    where
        order_reconciliation <> 'RECONCILIATION SUCCESSFUL'
        or order_item_count_recon = 'VARIANCE DETECTED'
        or order_status_recon = 'VARIANCE DETECTED'
)

select *
from failures
order by
    case existence_check
        when 'ERR: ORPHAN SUB-LEDGER' then 1
        when 'ERR: MISSING SUB-LEDGER' then 2
        else 3
    end,
    abs(item_count_variance) desc nulls last,
    total_order_amount desc
