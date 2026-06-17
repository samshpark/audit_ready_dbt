SELECT
    order_id,
    order_reconciliation,
    master_item_count,
    subledger_item_count
FROM {{ ref('order_reconciliation') }}
WHERE order_reconciliation != 'RECONCILIATION SUCCESSFUL'