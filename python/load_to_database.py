"""
Retail KPI Analytics System — ETL Loader
Loads generated CSV files into SQLite database for SQL analysis

Usage:
    python python/load_to_database.py

Requires: pandas, sqlalchemy
    pip install pandas sqlalchemy
"""

import pandas as pd
from sqlalchemy import create_engine
import os

DATA_DIR = "data/generated"
DB_PATH = "data/retail_kpi.db"

def load_table(engine, filepath, table_name, schema):
    df = pd.read_csv(filepath)
    df.to_sql(table_name, engine, schema=schema, 
              if_exists='replace', index=False)
    print(f"  Loaded {len(df):,} rows → {schema}.{table_name}")
    return len(df)

def main():
    engine = create_engine(f'sqlite:///{DB_PATH}')
    print(f"Loading data to {DB_PATH}")
    
    tables = [
        (f"{DATA_DIR}/dim_date.csv",         "dim_date",         "bronze"),
        (f"{DATA_DIR}/dim_store.csv",        "dim_store",        "bronze"),
        (f"{DATA_DIR}/dim_product.csv",      "dim_product",      "bronze"),
        (f"{DATA_DIR}/fact_transactions.csv","fact_transactions", "bronze"),
        (f"{DATA_DIR}/fact_labour.csv",      "fact_labour",       "bronze"),
        (f"{DATA_DIR}/fact_shrinkage.csv",   "fact_shrinkage",    "bronze"),
    ]
    
    total = 0
    for filepath, table_name, schema in tables:
        if os.path.exists(filepath):
            total += load_table(engine, filepath, table_name, schema)
        else:
            print(f"  WARNING: {filepath} not found — run generate_synthetic_data.py first")
    
    print(f"\nTotal rows loaded: {total:,}")
    print("Database ready. Open a SQL client pointed at:", DB_PATH)
    print("Or use: python -c \"import sqlite3; conn=sqlite3.connect('data/retail_kpi.db')\"")

if __name__ == '__main__':
    main()
