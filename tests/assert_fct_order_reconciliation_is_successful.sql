SELECT
    order_id,
    order_reconciliation,
    master_item_count,
    subledger_item_count
FROM {{ ref('fct_order_recon') }}
WHERE order_reconciliation != 'RECONCILIATION SUCCESSFUL'