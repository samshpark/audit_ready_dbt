import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. AUTHENTICATION & CLIENT SETUP
# Path to Google Cloud Service Account Key
KEY_PATH = "credentials/google_creds.json"

if not os.path.exists(KEY_PATH):
    raise FileNotFoundError(f"❌ Service account key not found at: {KEY_PATH}")

# Initialize BigQuery Client using the JSON key
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# 2. LOCAL STORAGE SETUP
# Ensure the 'data' directory exists to store  local 'snapshots'
os.makedirs("data", exist_ok=True)

def ingest_table(table_id, file_name):
    """
    Fetches data from BigQuery Public Dataset and saves it locally as a Parquet file.
    This mimics a real-world ETL/ELT 'Extract' phase.
    """
    print(f"🚀 [Ingestion] Starting extraction for table: '{table_id}'...")
    
    # SQL Query: Fetching from theLook eCommerce public dataset
    # We limit to 5,000 rows to keep local development fast and cost-effective.
    query = f"""
        SELECT * FROM `bigquery-public-data.thelook_ecommerce.{table_id}` 
        ORDER BY created_at DESC 
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
        
        print(f"✅ [Success] {output_path} saved successfully! ({len(df)} rows)")
    except Exception as e:
        print(f"❌ [Error] Failed to ingest {table_id}: {e}")

# 3. MAIN EXECUTION LOOP
if __name__ == "__main__":
    # Dictionary mapping: {BigQuery_Table_Name : Local_File_Name}
    target_tables = {
        "orders": "raw_orders",
        "order_items": "raw_order_items",
        "products": "raw_products",
        "users": "raw_users"
    }
    
    print("--- 🌟 Financial Data Pipeline: Ingestion Phase 🌟 ---")
    for bq_table, local_name in target_tables.items():
        ingest_table(bq_table, local_name)
    print("--- ✨ Ingestion Completed! Your local 'Data Lake' is ready. ✨ ---")