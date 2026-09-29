{{
    config(
        tags = ['financial', 'audit_ready']
    )
}}

-- Summarized general-ledger postings: one balanced journal entry per
-- posting date and entry type, as a debit line and a credit line.
-- Account mapping lives in the journal_entry_rules seed, so posting rules
-- change without touching SQL.

with journal_entry_lines as (
    select * from {{ ref('journal_entry_lines') }}
),

journal_entry_rules as (
    select * from {{ ref('journal_entry_rules') }}
),

chart_of_accounts as (
    select * from {{ ref('chart_of_accounts') }}
),

entry_totals as (
    select
        je_id,
        entry_type,
        posting_date,
        round(sum(amount), 2) as amount,
        count(*) as support_line_count
    from journal_entry_lines
    group by je_id, entry_type, posting_date
),

debit_lines as (
    select
        entry_totals.je_id,
        entry_totals.posting_date,
        entry_totals.entry_type,
        1 as line_number,
        journal_entry_rules.debit_account as account_code,
        entry_totals.amount as debit_amount,
        cast(0 as {{ dbt.type_float() }}) as credit_amount,
        entry_totals.support_line_count,
        journal_entry_rules.description
    from entry_totals
    inner join journal_entry_rules
        on entry_totals.entry_type = journal_entry_rules.entry_type
),

credit_lines as (
    select
        entry_totals.je_id,
        entry_totals.posting_date,
        entry_totals.entry_type,
        2 as line_number,
        journal_entry_rules.credit_account as account_code,
        cast(0 as {{ dbt.type_float() }}) as debit_amount,
        entry_totals.amount as credit_amount,
        entry_totals.support_line_count,
        journal_entry_rules.description
    from entry_totals
    inner join journal_entry_rules
        on entry_totals.entry_type = journal_entry_rules.entry_type
),

unioned as (
    select * from debit_lines
    union all
    select * from credit_lines
),

final as (
    select
        unioned.je_id,
        unioned.posting_date,
        unioned.entry_type,
        unioned.line_number,
        unioned.account_code,
        chart_of_accounts.account_name,
        chart_of_accounts.account_type,
        unioned.debit_amount,
        unioned.credit_amount,
        unioned.support_line_count,
        unioned.description
    from unioned
    inner join chart_of_accounts
        on unioned.account_code = chart_of_accounts.account_code
)

select * from final
