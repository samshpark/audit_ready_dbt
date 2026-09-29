-- Control totals: posted GL amounts must agree with an independently built
-- mart for each entry type. Revenue is posted from the order-grain revenue
-- mart but checked against the item-grain subledger; returns against
-- refund_reconciliation; COGS against inventory_fiscal_report, which rounds
-- per product-year and so needs a small rounding tolerance.

with posted as (
    select
        entry_type,
        sum(debit_amount) as posted_amount
    from {{ ref('journal_entries') }}
    group by entry_type
),

expected as (
    select
        'REV' as entry_type,
        sum(recognized_item_revenue) as expected_amount,
        0.01 as tolerance
    from {{ ref('order_item_revenue') }}
    union all
    select
        'RET' as entry_type,
        sum(refund_amount) as expected_amount,
        0.01 as tolerance
    from {{ ref('refund_reconciliation') }}
    union all
    select
        'COGS' as entry_type,
        sum(period_cogs_amount) as expected_amount,
        5.00 as tolerance
    from {{ ref('inventory_fiscal_report') }}
)

select
    expected.entry_type,
    posted.posted_amount,
    expected.expected_amount,
    coalesce(posted.posted_amount, 0) - expected.expected_amount as difference
from expected
left join posted
    on expected.entry_type = posted.entry_type
where
    abs(coalesce(posted.posted_amount, 0) - expected.expected_amount)
    > expected.tolerance
