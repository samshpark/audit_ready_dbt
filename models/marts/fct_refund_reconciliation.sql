{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

WITH reconciled AS (
    SELECT
        order_id,
        user_id,
        first_item_created_at AS order_date,
        total_order_amount AS gross_revenue,
        refund_amount,
        subledger_item_count AS total_item_count,
        returned_item_count AS total_returned_items
    FROM {{ ref('int_order_items_aggregated') }}
    {% if is_incremental() %}
    -- Only process orders touched by the most recent int_order_items_summary incremental run
    WHERE dbt_updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
    {% endif %}
    -- FROM {{ ref('seed_order_items') }} - Used for the testing of the sql for partially refunded scenario
),

final AS (
    SELECT 
        *,
        (gross_revenue - refund_amount) AS net_revenue,
        ROUND(COALESCE(refund_amount / NULLIF(gross_revenue, 0), 0), 4) AS refund_value_rate,
        ROUND(COALESCE(CAST(total_returned_items AS FLOAT) / NULLIF(total_item_count, 0), 0), 4) AS refund_count_rate,
        CASE 
            WHEN total_returned_items = 0 THEN 'NO REFUND'
            WHEN total_returned_items = total_item_count THEN 'FULLY REFUNDED'
            ELSE 'PARTIALLY REFUNDED'
        END AS refund_type
    FROM reconciled
)

SELECT * FROM final
--WHERE refund_type = 'PARTIALLY REFUNDED' : - Used for the testing of the sql for partially refunded scenario