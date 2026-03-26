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

# Function 
def ingest_table(table_id, file_name, sort_column):
    """
    Extracts data from BigQuery and saves it as a local Parquet file.
    Uses a dynamic sort_column to handle different table schemas.
    """
    print(f"[Ingestion] Starting extraction for table: '{table_id}'...")
    
    # SQL Query: Fetching from theLook eCommerce public dataset
    # Dynamic Query: Uses the specific sort_column for each table
    # We limit to 5,000 rows to keep local development fast and cost-effective.
    query = f"""
        SELECT * FROM `bigquery-public-data.thelook_ecommerce.{table_id}` 
        ORDER BY {sort_column} DESC 
        LIMIT 5000
    """
    
    try:
        # Execute Query & Convert to Pandas DataFrame
        # 'db-dtypes' is required for BigQuery to Pandas conversion
        df = client.query(query).to_dataframe()
        
        # Save as Parquet (Recommended for AE portfolios)
        # Parquet preserves data types (integers, timestamps) better than CSV.
        output_path = f"data/{file_name}.parquet"
        df.to_parquet(output_path, index=False)
        
        print(f"[Success] {output_path} saved successfully! ({len(df)} rows)")
    except Exception as e:
        print(f"[Error] Failed to ingest {table_id}: {e}")

# 3. MAIN EXECUTION LOOP
if __name__ == "__main__":
    # Dictionary mapping: {BigQuery_Table_Name : (Local_File_Name, Sort_column)}
    # Products table uses 'id' instead of 'created_at'
    target_tables = {
        "orders": ("raw_orders", "created_at"),
        "order_items": ("raw_order_items", "created_at"),
        "products": ("raw_products", "id"),
        "users": ("raw_users", "created_at")
    }
    
    print("--- Financial Data Pipeline: Ingestion Phase ---")
    for bq_table, (local_name, sort_col) in target_tables.items():
        ingest_table(bq_table, local_name, sort_col)
    print("--- Ingestion Completed! Your local 'Data Lake' is ready. ---")