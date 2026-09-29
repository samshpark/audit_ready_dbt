-- Preventive control: no order, shipment, or return may be dated after the
-- build runs. A future shipped_at means revenue.sql recognizes revenue for a
-- shipment that has not happened, and journal_entries would post it. Error
-- severity, so on the daily Athena branch a failure stops the build before
-- export_journal_entries posts anything.

with events as (
    select
        'revenue.created_at' as event,
        r.order_id as reference,
        r.created_at as event_at
    from {{ ref('revenue') }} as r
    union all
    select
        'revenue.shipped_at' as event,
        r.order_id as reference,
        r.shipped_at as event_at
    from {{ ref('revenue') }} as r
    union all
    select
        'order_item_revenue.shipped_at' as event,
        i.order_item_id as reference,
        i.shipped_at as event_at
    from {{ ref('order_item_revenue') }} as i
    union all
    select
        'order_item_revenue.returned_at' as event,
        i.order_item_id as reference,
        i.returned_at as event_at
    from {{ ref('order_item_revenue') }} as i
)

select
    events.event,
    events.reference,
    events.event_at
from events
where events.event_at > current_timestamp
