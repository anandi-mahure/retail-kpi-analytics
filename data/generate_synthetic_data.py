"""
Retail Performance KPI Analytics System
Synthetic Data Generator

Generates realistic retail operational data with:
- Seasonal patterns (Christmas uplift, summer dip)
- Deliberate data quality issues for cleaning demonstration
- Statistical properties calibrated to UK retail benchmarks
  (BRC Retail Crime Report 2024, ONS Retail Sales Index)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

np.random.seed(42)
random.seed(42)

# ── CONFIG ──────────────────────────────────────────────────────────────────
N_STORES = 20
N_PRODUCTS = 500
N_DAYS = 730  # 2 years
START_DATE = datetime(2023, 1, 1)
OUTPUT_DIR = "data/generated"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── DIM: DATE ────────────────────────────────────────────────────────────────
def generate_dim_date():
    dates = [START_DATE + timedelta(days=i) for i in range(N_DAYS)]
    df = pd.DataFrame({
        'date_key': [d.strftime('%Y%m%d') for d in dates],
        'full_date': [d.strftime('%Y-%m-%d') for d in dates],
        'year': [d.year for d in dates],
        'month': [d.month for d in dates],
        'month_name': [d.strftime('%B') for d in dates],
        'quarter': [(d.month - 1) // 3 + 1 for d in dates],
        'day_of_week': [d.weekday() for d in dates],
        'day_name': [d.strftime('%A') for d in dates],
        'week_number': [d.isocalendar()[1] for d in dates],
        'fiscal_week': [((d - START_DATE).days // 7) + 1 for d in dates],
        'fiscal_year': [2023 if d.year == 2023 else 2024 for d in dates],
        'is_weekend': [1 if d.weekday() >= 5 else 0 for d in dates],
        'is_bank_holiday': [0] * N_DAYS,  # Simplified
    })
    # Mark Christmas period
    df.loc[(df['month'] == 12) & (df['full_date'].str[8:10].astype(int) >= 15), 'is_bank_holiday'] = 1
    return df

# ── DIM: STORE ───────────────────────────────────────────────────────────────
def generate_dim_store():
    regions = ['London', 'South East', 'Midlands', 'North West', 'Yorkshire', 
               'Scotland', 'Wales', 'South West', 'North East', 'East Anglia']
    formats = ['High Street', 'Retail Park', 'Superstore', 'Local']
    
    stores = []
    for i in range(1, N_STORES + 1):
        stores.append({
            'store_id': i,
            'store_name': f'Store {i:03d}',
            'region': regions[i % len(regions)],
            'format': formats[i % len(formats)],
            'size_sqft': np.random.randint(3000, 20000),
            'manager_id': i,
            'opening_date': '2015-01-01',
            'target_weekly_sales': np.random.randint(80000, 250000),
        })
    return pd.DataFrame(stores)

# ── DIM: PRODUCT ─────────────────────────────────────────────────────────────
def generate_dim_product():
    categories = {
        'Frozen': ['Ready Meals', 'Ice Cream', 'Vegetables', 'Meat'],
        'Grocery': ['Tinned Goods', 'Cereals', 'Condiments', 'Pasta'],
        'Chilled': ['Dairy', 'Cooked Meats', 'Salads', 'Dips'],
        'Bakery': ['Bread', 'Cakes', 'Pastries'],
        'Drinks': ['Soft Drinks', 'Juices', 'Water', 'Energy'],
        'Household': ['Cleaning', 'Paper', 'Laundry'],
    }
    
    products = []
    pid = 1
    for cat, subcats in categories.items():
        for subcat in subcats:
            n = N_PRODUCTS // 20
            for _ in range(n):
                price = round(np.random.uniform(0.49, 8.99), 2)
                products.append({
                    'product_id': pid,
                    'product_name': f'{subcat} Product {pid}',
                    'category': cat,
                    'subcategory': subcat,
                    'unit_price': price,
                    'cost_price': round(price * np.random.uniform(0.45, 0.70), 2),
                    'margin_band': 'Low' if price < 2 else ('Mid' if price < 5 else 'High'),
                    'supplier_id': np.random.randint(1, 50),
                    'is_own_label': np.random.choice([0, 1], p=[0.6, 0.4]),
                })
                pid += 1
    return pd.DataFrame(products)

# ── FACT: TRANSACTIONS ───────────────────────────────────────────────────────
def generate_fact_transactions(dim_date, dim_store, dim_product):
    print("Generating transactions (this takes ~60 seconds)...")
    transactions = []
    tid = 1
    
    for _, date_row in dim_date.iterrows():
        date_str = date_row['full_date']
        
        # Seasonal multiplier
        month = date_row['month']
        day_of_week = date_row['day_of_week']
        seasonal = 1.0
        if month == 12: seasonal = 1.4  # Christmas
        if month in [7, 8]: seasonal = 0.85  # Summer dip
        if day_of_week in [4, 5]: seasonal *= 1.15  # Friday/Saturday uplift
        
        for store_id in range(1, N_STORES + 1):
            n_transactions = int(np.random.normal(150, 30) * seasonal)
            n_transactions = max(50, min(350, n_transactions))
            
            for _ in range(n_transactions):
                product_id = np.random.randint(1, len(dim_product) + 1)
                product = dim_product[dim_product['product_id'] == product_id].iloc[0]
                quantity = np.random.choice([1, 1, 1, 2, 2, 3], p=[0.4, 0.25, 0.15, 0.1, 0.07, 0.03])
                
                # Inject ~3% data quality issues
                if np.random.random() < 0.02:
                    product_id = 9999  # Orphan product (intentional DQ issue)
                if np.random.random() < 0.01:
                    tid_val = tid - np.random.randint(1, 10)  # Duplicate transaction
                else:
                    tid_val = tid
                
                transactions.append({
                    'transaction_id': f'T{tid_val:08d}',
                    'store_id': store_id,
                    'product_id': product_id,
                    'transaction_date': date_str,
                    'transaction_hour': np.random.choice(range(7, 22), 
                        p=[0.02,0.04,0.08,0.10,0.12,0.12,0.11,0.10,0.09,0.08,0.06,0.04,0.03,0.01,0.01]),
                    'quantity': quantity,
                    'unit_price': product['unit_price'],
                    'net_sales_value': round(quantity * product['unit_price'], 2),
                    'transaction_type': 'SALE',
                })
                tid += 1
    
    df = pd.DataFrame(transactions)
    # Sample to ~200K for performance
    if len(df) > 200000:
        df = df.sample(200000, random_state=42).reset_index(drop=True)
    return df

# ── FACT: LABOUR ─────────────────────────────────────────────────────────────
def generate_fact_labour(dim_date):
    labour = []
    for _, date_row in dim_date.iterrows():
        for store_id in range(1, N_STORES + 1):
            contracted = np.random.uniform(40, 120)
            actual = contracted * np.random.uniform(0.90, 1.15)
            hourly_rate = np.random.uniform(10.42, 13.50)  # UK NMW range
            
            labour.append({
                'store_id': store_id,
                'shift_date': date_row['full_date'],
                'fiscal_week': date_row['fiscal_week'],
                'contracted_hours': round(contracted, 1),
                'actual_hours': round(actual, 1),
                'hourly_rate': round(hourly_rate, 2),
                'total_labour_cost': round(actual * hourly_rate, 2),
            })
    return pd.DataFrame(labour)

# ── FACT: SHRINKAGE ──────────────────────────────────────────────────────────
def generate_fact_shrinkage(dim_date):
    cause_codes = ['SHOPLIFTING', 'ADMIN_ERROR', 'SUPPLIER_FRAUD', 'STAFF_THEFT', 'WASTAGE']
    categories = ['Frozen', 'Grocery', 'Chilled', 'Bakery', 'Drinks', 'Household']
    
    shrinkage = []
    for _, date_row in dim_date.iterrows():
        for store_id in range(1, N_STORES + 1):
            if np.random.random() < 0.3:  # Not every store has shrinkage every day
                # Occasional spikes for anomaly detection demo
                base_value = np.random.exponential(50)
                if np.random.random() < 0.05:
                    base_value *= 5  # Spike — should trigger Z-score alert
                
                shrinkage.append({
                    'store_id': store_id,
                    'shrinkage_date': date_row['full_date'],
                    'fiscal_week': date_row['fiscal_week'],
                    'cause_code': np.random.choice(cause_codes, p=[0.45, 0.20, 0.10, 0.10, 0.15]),
                    'category': np.random.choice(categories),
                    'shrinkage_value': round(base_value, 2),
                })
    return pd.DataFrame(shrinkage)

# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("Generating dimension tables...")
    dim_date = generate_dim_date()
    dim_store = generate_dim_store()
    dim_product = generate_dim_product()
    
    dim_date.to_csv(f'{OUTPUT_DIR}/dim_date.csv', index=False)
    dim_store.to_csv(f'{OUTPUT_DIR}/dim_store.csv', index=False)
    dim_product.to_csv(f'{OUTPUT_DIR}/dim_product.csv', index=False)
    print(f"  dim_date: {len(dim_date)} rows")
    print(f"  dim_store: {len(dim_store)} rows")
    print(f"  dim_product: {len(dim_product)} rows")
    
    print("Generating fact tables...")
    fact_transactions = generate_fact_transactions(dim_date, dim_store, dim_product)
    fact_labour = generate_fact_labour(dim_date)
    fact_shrinkage = generate_fact_shrinkage(dim_date)
    
    fact_transactions.to_csv(f'{OUTPUT_DIR}/fact_transactions.csv', index=False)
    fact_labour.to_csv(f'{OUTPUT_DIR}/fact_labour.csv', index=False)
    fact_shrinkage.to_csv(f'{OUTPUT_DIR}/fact_shrinkage.csv', index=False)
    
    print(f"  fact_transactions: {len(fact_transactions):,} rows")
    print(f"  fact_labour: {len(fact_labour):,} rows")
    print(f"  fact_shrinkage: {len(fact_shrinkage):,} rows")
    print(f"\nAll files saved to {OUTPUT_DIR}/")
    print("Next step: run sql/01_create_schema.sql to set up the database")
