-- Double-entry integrity: every journal entry's debits must equal its
-- credits. Returns the entries that do not balance to the cent.
select
    je_id,
    sum(debit_amount) as total_debits,
    sum(credit_amount) as total_credits
from {{ ref('journal_entries') }}
group by je_id
having abs(sum(debit_amount) - sum(credit_amount)) >= 0.005
