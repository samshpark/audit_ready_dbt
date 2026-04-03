WITH inbound AS ( 
    SELECT
        inventory_item_id,
        product_id,
        product_category,
        product_name,
        product_brand,
        created_at AS inbound_at,
        cost, 
        product_retail_price,
        product_department
    FROM
        {{ ref('stg_inventory_items') }}
),

outbound AS (
    SELECT 
        inventory_item_id,
        product_id,
        shipped_at AS outbound_at,
        sale_price
    FROM
        {{ ref('stg_order_items') }}
    WHERE
        order_item_status NOT IN ('cancelled', 'returned', 'processing') -- only include 'shipped' & 'complete'
),

final AS (
    SELECT
        COALESCE(inb.inventory_item_id, outb.inventory_item_id) AS inventory_item_id,
        COALESCE(inb.product_id, outb.product_id) AS product_id,
        inb.product_category,
        inb.product_name,
        inb.product_brand, 
        inb.inbound_at,
        outb.outbound_at,
        inb.cost AS historical_unit_cost,
        inb.product_retail_price AS current_market_price,
        outb.sale_price,
        LEAST(inb.cost, inb.product_retail_price) AS lcm_unit_valuation, -- Evlaute LCM (Low cost or market value)
        -- "Allowance for Inventory Valuation" in Financial Statements
        inb.cost - LEAST(inb.cost, inb.product_retail_price) AS inventory_valuation_loss,
        
        CASE 
            WHEN inb.inbound_at IS NULL THEN NULL 
            WHEN outb.outbound_at IS NOT NULL 
            THEN DATEDIFF('day', CAST(inb.inbound_at AS DATE), CAST(outb.outbound_at AS DATE))
            -- 미래 입고분은 아직 보유일수가 0일인 것으로 처리
            ELSE GREATEST(DATEDIFF('day', CAST(inb.inbound_at AS DATE), CURRENT_DATE), 0)
        END AS days_in_inventory,

        CASE WHEN inb.inbound_at IS NULL THEN NULL 
            WHEN outb.outbound_at IS NOT NULL 
            THEN DATEDIFF('day', CAST(inb.inbound_at AS DATE), CAST(outb.outbound_at AS DATE))
            ELSE DATEDIFF('day', CAST(inb.inbound_at AS DATE), CURRENT_DATE)
        END AS days_in_inventory,

        CASE 
            -- 1. Ending Inventory
            WHEN outb.outbound_at IS NULL THEN 
                CASE 
                    WHEN days_in_inventory > 365 THEN 'On-hand: 04. Obsolete (>1yr)'
                    WHEN days_in_inventory > 180 THEN 'On-hand: 03. Slow-moving (6m-1yr)'
                    WHEN days_in_inventory > 90  THEN 'On-hand: 02. Stagnant (3m-6m)'
                    ELSE 'On-hand: 01. Healthy (<3m)' 
                END
    
            -- 2. COGs
            ELSE 
                CASE 
                    WHEN days_in_inventory > 365 THEN 'Sold: 04. Very Slow (>1yr)'
                    WHEN days_in_inventory > 180 THEN 'Sold: 03. Slow (6m-1yr)'
                    WHEN days_in_inventory > 90  THEN 'Sold: 02. Normal (3m-6m)'
                    ELSE 'Sold: 01. Fast (<3m)'
                END
        END AS aging_velocity_bucket,

        CASE WHEN inb.inventory_item_id IS NULL THEN 'Error: Outbound without Inbound'
            WHEN outb.outbound_at IS NULL THEN 'On-hand (Ending Inventory)'
            ELSE 'Sold (COGS)'
        END AS stock_status,

        -- Evaluate whether the product_id is a match
        CASE WHEN inb.product_id <> outb.product_id THEN 'Product Not Match' ELSE 'Product Match' END AS product_match
    FROM inbound inb
    FULL JOIN outbound outb
        ON inb.inventory_item_id = outb.inventory_item_id
)

SELECT * FROM final