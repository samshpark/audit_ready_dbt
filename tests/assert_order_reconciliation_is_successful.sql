select
    order_id,
    order_reconciliation,
    master_item_count,
    subledger_item_count
from {{ ref('order_reconciliation') }}
where order_reconciliation <> 'RECONCILIATION SUCCESSFUL'
