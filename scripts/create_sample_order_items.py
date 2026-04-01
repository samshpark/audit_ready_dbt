import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# Load actual data for sample user_id, product_id, inventory_item_id
item_path = 'data/raw_order_items.parquet'
user_path = 'data/raw_users.parquet'

user_df = pd.read_parquet(user_path)
item_df = pd.read_parquet(item_path)

# Retrieve user_id, item_id & producted_id list - to maintain the relationship between the two fields. 
real_user_ids = user_df['id'].unique().tolist()
real_item_pairs = item_df[['product_id', 'inventory_item_id']].drop_duplicates().values.tolist()

# function to get random date - retured date
def get_random_created_at():
    """Create random created at between 2026.3.1 ~ 2026.3.25"""
    start_base = datetime(2026, 3, 1, 0, 0, 0)
    # Assign random date
    random_minutes = random.randint(0, 36000)
    return start_base + timedelta(minutes=random_minutes)

# function to format date
def fmt(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S') if dt else None

# To get data for 5000 orders
rows = 5000
data = []

for i in range(rows):
    order_id = -1 * (i + 1)
    user_id = random.choice(real_user_ids)
    order_created_at = get_random_created_at()

    order_type = np.random.choice(
        ['FULLY REFUNDED', 'PARTIALLY REFUNDED', 'NO REFUND'],
        p = [0.07, 0.15, 0.78]
    )
    
    for j in range(2):
        item_id = -1 * (len(data)+1)
        product_id, inventory_item_id = random.choice(real_item_pairs)

        # Define date 
        # 1. Shipped: Created + 1~2 days
        shipped_at = order_created_at + timedelta(minutes=random.randint(1440, 2880))
        
        # 2. Delivered: Shipped + 1~3 days
        delivered_at = shipped_at + timedelta(minutes=random.randint(1440, 4320))
        
        # 3. Returned: Delivered + 1~7 days if refunded only
        returned_at = None
        
        if order_type == 'FULLY REFUNDED':
            status = 'returned'
            returned_at = delivered_at + timedelta(minutes=random.randint(1440, 10080))
        elif order_type == 'PARTIALLY REFUNDED':
            status = 'returned' if j == 1 else 'complete'
            if status == 'returned':
                returned_at = delivered_at + timedelta(minutes=random.randint(1440, 10080))
        else:
            status = np.random.choice(['complete', 'shipped'], p=[0.9, 0.1])
            # If shipping in progress, remove delievered_at
            if status == 'shipped':
                delivered_at = None

        data.append({
            "id": item_id,
            "order_id": order_id,
            "user_id": user_id,
            "product_id": product_id,
            "inventory_item_id": inventory_item_id,
            "status": status,
            "created_at": fmt(order_created_at),
            "shipped_at": fmt(shipped_at),
            "delivered_at": fmt(delivered_at),
            "returned_at": fmt(returned_at),
            "sale_price": np.random.randint(20, 200)
        })

df = pd.DataFrame(data)
df.to_csv('seeds/data_test_order_items.csv', index=False)
print(f" {len(df)} rows generated successfully in seeds/data_test_order_items.csv!")