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

data_cleanse AS (
    SELECT
        COALESCE(inb.inventory_item_id, outb.inventory_item_id) AS inventory_item_id,
        COALESCE(inb.product_id, outb.product_id) AS product_id,
        inb.product_id AS inbound_product_id, 
        outb.product_id AS outbound_product_id, 
        inb.product_category,
        inb.product_name,
        inb.product_brand,

        /* [DATA INTEGRITY: CHRONOLOGY CLEANSE]
        Ensures Inbound (created_at) <= Outbound (shipped_at).
        
        Rationale: 
        - Physically impossible for an item to be sold before it is received.
        - Prevents negative 'days_in_inventory'.
        - Eliminates 'audit_check_diff' errors in P&L (Beginning + Purchase - Ending = COGS).
        
        Strategy: 
        Filtering at the INTERMEDIATE layer to provide 'Audit-Ready' data to Marts 
        while keeping Staging as a raw mirror.
        */
        CASE 
            WHEN outb.outbound_at IS NOT NULL AND inb.inbound_at > outb.outbound_at 
            THEN outb.outbound_at 
            ELSE inb.inbound_at 
        END AS inbound_at,

        outb.outbound_at,
        inb.cost AS historical_unit_cost,
        inb.product_retail_price AS current_market_price,
        outb.sale_price
    FROM inbound inb
    FULL JOIN outbound outb
        ON inb.inventory_item_id = outb.inventory_item_id
),

final AS (
    SELECT
        inventory_item_id,
        product_id,
        product_category,
        product_name,
        product_brand, 
        inbound_at,        
        outbound_at,
        historical_unit_cost,
        current_market_price,
        sale_price,
        LEAST(historical_unit_cost, current_market_price) AS lcm_unit_valuation, -- Evlaute LCM (Low cost or market value)
        -- "Allowance for Inventory Valuation" in Financial Statements
        historical_unit_cost - LEAST(historical_unit_cost, current_market_price) AS inventory_valuation_loss,
        
        CASE 
            WHEN inbound_at IS NULL THEN NULL 
            WHEN outbound_at IS NOT NULL 
            THEN DATEDIFF('day', CAST(inbound_at AS DATE), CAST(outbound_at AS DATE))
            -- 0 if future inbound
            ELSE GREATEST(DATEDIFF('day', CAST(inbound_at AS DATE), CURRENT_DATE), 0)
        END AS days_in_inventory,

        CASE 
            -- 1. Ending Inventory
            WHEN outbound_at IS NULL THEN 
                CASE 
                    WHEN days_in_inventory > 365*4 THEN 'On-hand: 04. Obsolete (>4yr)'
                    WHEN days_in_inventory > 365*3 THEN 'On-hand: 03. Slow-moving (3yr-4yr)'
                    WHEN days_in_inventory > 365*2  THEN 'On-hand: 02. Stagnant (2yr-3yr)'
                    ELSE 'On-hand: 01. Healthy (<2y)' 
                END
    
            -- 2. COGs
            ELSE 
                CASE 
                    WHEN days_in_inventory > 365*4 THEN 'Sold: 04. Very Slow (>4yr)'
                    WHEN days_in_inventory > 365*3 THEN 'Sold: 03. Slow (3yr-4yr)'
                    WHEN days_in_inventory > 365*2  THEN 'Sold: 02. Normal (2yr-3yr)'
                    ELSE 'Sold: 01. Fast (<2y)'
                END
        END AS aging_velocity_bucket,

        CASE WHEN inventory_item_id IS NULL THEN 'Error: Outbound without Inbound'
            WHEN outbound_at IS NULL THEN 'On-hand (Ending Inventory)'
            ELSE 'Sold (COGS)'
        END AS stock_status,

        -- Evaluate whether the product_id is a match
        CASE
            WHEN inbound_product_id IS NOT NULL
                AND outbound_product_id IS NOT NULL
                AND inbound_product_id <> outbound_product_id
            THEN 'Product Not Match'
            ELSE 'Product Match'
        END AS product_match,
        EXTRACT(YEAR FROM outbound_at::timestamp) AS outbound_fiscal_year,
        EXTRACT(YEAR FROM inbound_at::timestamp) AS inbound_fiscal_year

    FROM data_cleanse
)

SELECT * FROM final