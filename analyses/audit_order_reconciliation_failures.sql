-- Surfaces all order-level reconciliation failures between the master ledger
-- (raw_orders) and sub-ledger (raw_order_items) for auditor follow-up:
--   1. Orphan sub-ledger: line items exist with no matching order header
--   2. Missing sub-ledger: order header exists with no matching line items
--   3. Item count variance: master and sub-ledger disagree on unit count
--   4. Status variance: order status differs between header and line level
--
-- Usage: dbt compile -s analyses/audit_order_reconciliation_failures
--        then copy SQL from target/compiled/ and run directly

WITH recon AS (
    SELECT * FROM {{ ref('order_reconciliation') }}
),

failures AS (
    SELECT
        order_id,
        user_id,
        order_at,
        total_order_amount,
        master_item_count,
        subledger_item_count,
        master_item_count - subledger_item_count AS item_count_variance,
        order_reconciliation    AS existence_check,
        order_item_count_recon  AS item_count_check,
        order_status_recon      AS status_check

    FROM recon
    WHERE order_reconciliation != 'RECONCILIATION SUCCESSFUL'
       OR order_item_count_recon = 'VARIANCE DETECTED'
       OR order_status_recon = 'VARIANCE DETECTED'
)

SELECT *
FROM failures
ORDER BY
    CASE existence_check
        WHEN 'ERR: ORPHAN SUB-LEDGER'  THEN 1
        WHEN 'ERR: MISSING SUB-LEDGER' THEN 2
        ELSE 3
    END,
    ABS(item_count_variance) DESC NULLS LAST,
    total_order_amount DESC
