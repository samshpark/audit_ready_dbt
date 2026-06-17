-- Item count variance is always unexpected.
-- Status variance has two expected patterns that are excluded:
--   1. Partially refunded: master stays 'complete', subledger aggregates to 'complete,returned'
--   2. Mixed shipping progress: master is 'shipped' while some items already 'complete',
--      producing a 'complete,shipped' subledger agg — a normal multi-item shipping scenario.
SELECT
    r.order_id,
    r.master_item_count,
    r.subledger_item_count,
    r.order_item_count_recon,
    r.order_status_recon
FROM {{ ref('order_reconciliation') }} r
LEFT JOIN {{ ref('int_order_items_aggregated') }} i ON r.order_id = i.order_id
WHERE r.order_item_count_recon = 'VARIANCE DETECTED'
   OR (
       r.order_status_recon = 'VARIANCE DETECTED'
       AND NOT (i.returned_item_count > 0 AND i.returned_item_count < i.subledger_item_count)
       AND NOT (
           i.order_item_status_agg LIKE '%shipped%'
           AND i.order_item_status_agg LIKE '%complete%'
           AND i.returned_item_count = 0
       )
   )