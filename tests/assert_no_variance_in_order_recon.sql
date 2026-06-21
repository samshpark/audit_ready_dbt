{#
    Item count variance is always unexpected.
    Status variance has two expected patterns that are excluded:
      1. Partially refunded: master stays 'complete', subledger aggregates to
         'complete,returned'
      2. Mixed shipping progress: master is 'shipped' while some items already
         'complete', producing a 'complete,shipped' subledger agg — a normal
         multi-item shipping scenario.
#}
select
    r.order_id,
    r.master_item_count,
    r.subledger_item_count,
    r.order_item_count_recon,
    r.order_status_recon
from {{ ref('order_reconciliation') }} as r
left join
    {{ ref('int_order_items_aggregated') }} as i
    on r.order_id = i.order_id
where
    r.order_item_count_recon = 'VARIANCE DETECTED'
    or (
        r.order_status_recon = 'VARIANCE DETECTED'
        and not (
            i.returned_item_count > 0
            and i.returned_item_count < i.subledger_item_count
        )
        and not (
            i.order_item_status_list like '%shipped%'
            and i.order_item_status_list like '%complete%'
            and i.returned_item_count = 0
        )
    )
