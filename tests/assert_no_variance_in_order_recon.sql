SELECT
    order_id,
    master_item_count,
    subledger_item_count,
    order_item_count_recon,
    order_status_recon
FROM {{ ref('fct_order_recon') }}
WHERE order_item_count_recon = 'VARIANCE DETECTED'
   OR order_status_recon = 'VARIANCE DETECTED'