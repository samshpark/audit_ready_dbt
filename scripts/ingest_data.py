import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. AUTHENTICATION & CLIENT SETUP
# Path to Google Cloud Service Account Key
KEY_PATH = "credentials/google_creds.json"

# Check if the credential file exists
if not os.path.exists(KEY_PATH):
    raise FileNotFoundError(f"Service account key not found at: {KEY_PATH}")
    sys.exit(1)

# Initialize BigQuery Client using the JSON key
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# 2. LOCAL STORAGE SETUP
# Ensure the 'data' directory exists to store  local 'snapshots'
os.makedirs("data", exist_ok=True)

def save_to_parquet(df, file_name):
    if df is not None and not df.empty:
        output_path = f"data/{file_name}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"[Ingestion Success] {output_path} saved! ({len(df)} rows)")
        return df
    return None

# 3. STRATEGIC INGESTION
if __name__ == "__main__": # Execute the following block only if the script is run directly
    print("[SYSTEM] Starting Financial Data Pipeline: Full Referential Integrity Mode")

    # [Step 1] Orders: Choose random 1000 product and get all orders related to the product
    orders_query = """
    WITH random_products AS (
    SELECT DISTINCT product_id
    FROM `bigquery-public-data.thelook_ecommerce.order_items`
    ORDER BY RAND()
    LIMIT 1000
    ),

    target_order_ids AS (
        SELECT DISTINCT oi.order_id
        FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
        INNER JOIN random_products rp ON oi.product_id = rp.product_id
    )

    SELECT o.*
    FROM `bigquery-public-data.thelook_ecommerce.orders` o
    INNER JOIN target_order_ids t ON o.order_id = t.order_id
    ORDER BY o.created_at DESC                              
    """

    df_orders = client.query(orders_query).to_dataframe()
    save_to_parquet(df_orders, "raw_orders")

    if not df_orders.empty:
        # [Step 2] Order Items: get order items that are in [Step 1] query 
        order_ids = ", ".join(map(str, df_orders['order_id'].tolist()))
        items_query = f"SELECT * FROM `bigquery-public-data.thelook_ecommerce.order_items` WHERE order_id IN ({order_ids})"
        df_items = client.query(items_query).to_dataframe()
        save_to_parquet(df_items, "raw_order_items")

        # [Step 3] Inventory Item: get user info that are in [Step 1] query
        if not df_items.empty:
                product_ids = ", ".join(map(str, df_items['product_id'].unique().tolist()))
        
                inventory_item_query = f"""
                        SELECT * FROM `bigquery-public-data.thelook_ecommerce.inventory_items` 
                        WHERE product_id IN ({product_ids})
                    """
                df_inventory_items = client.query(inventory_item_query).to_dataframe()
                save_to_parquet(df_inventory_items, "raw_inventory_items")

        # [Step 4] Users: get user info that are in [Step 1] query
        user_ids = ", ".join(map(str, df_orders['user_id'].unique().tolist()))
        users_query = f"SELECT * FROM `bigquery-public-data.thelook_ecommerce.users` WHERE id IN ({user_ids})"
        df_users = client.query(users_query).to_dataframe()
        save_to_parquet(df_users, "raw_users")

        if not df_items.empty:
            product_ids = ", ".join(map(str, df_items['product_id'].unique().tolist()))
            
            # [Step 5] Products: get product info that are associated with order_items above
            products_query = f"SELECT * FROM `bigquery-public-data.thelook_ecommerce.products` WHERE id IN ({product_ids})"
            df_products = client.query(products_query).to_dataframe()
            save_to_parquet(df_products, "raw_products")

    print("\n--- Ingestion Completed: All tables are perfectly linked! ---")