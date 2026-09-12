-- Aliased explicitly: the table and the order_reconciliation column share a
-- name, and BigQuery resolves an unqualified reference to the unaliased
-- table as the whole row (STRUCT) rather than the column in that case.
select
    t.order_id,
    t.order_reconciliation,
    t.master_item_count,
    t.subledger_item_count
from {{ ref('order_reconciliation') }} as t
where t.order_reconciliation <> 'RECONCILIATION SUCCESSFUL'
