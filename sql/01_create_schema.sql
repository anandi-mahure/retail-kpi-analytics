-- ============================================================
-- RETAIL KPI ANALYTICS SYSTEM
-- Script 01: Schema Creation
-- Run this first before any other scripts
-- ============================================================

-- Bronze schema: raw ingested data, never modified
CREATE SCHEMA IF NOT EXISTS bronze;
-- Silver schema: cleaned and validated data
CREATE SCHEMA IF NOT EXISTS silver;
-- Gold schema: star schema for Power BI
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze: raw transactions
CREATE TABLE bronze.fact_transactions (
    transaction_id      VARCHAR(12),
    store_id            INT,
    product_id          INT,
    transaction_date    DATE,
    transaction_hour    INT,
    quantity            INT,
    unit_price          DECIMAL(8,2),
    net_sales_value     DECIMAL(10,2),
    transaction_type    VARCHAR(10),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_flag   VARCHAR(50) DEFAULT 'PENDING'
);

CREATE TABLE bronze.dim_date (
    date_key INT, full_date DATE, year INT, month INT,
    month_name VARCHAR(10), quarter INT, day_of_week INT,
    day_name VARCHAR(10), week_number INT, fiscal_week INT,
    fiscal_year INT, is_weekend INT, is_bank_holiday INT
);

CREATE TABLE bronze.dim_store (
    store_id INT, store_name VARCHAR(50), region VARCHAR(50),
    format VARCHAR(20), size_sqft INT, manager_id INT,
    opening_date DATE, target_weekly_sales DECIMAL(12,2)
);

CREATE TABLE bronze.dim_product (
    product_id INT, product_name VARCHAR(100), category VARCHAR(50),
    subcategory VARCHAR(50), unit_price DECIMAL(8,2),
    cost_price DECIMAL(8,2), margin_band VARCHAR(10),
    supplier_id INT, is_own_label INT
);

CREATE TABLE bronze.fact_labour (
    store_id INT, shift_date DATE, fiscal_week INT,
    contracted_hours DECIMAL(6,1), actual_hours DECIMAL(6,1),
    hourly_rate DECIMAL(6,2), total_labour_cost DECIMAL(10,2)
);

CREATE TABLE bronze.fact_shrinkage (
    store_id INT, shrinkage_date DATE, fiscal_week INT,
    cause_code VARCHAR(20), category VARCHAR(50),
    shrinkage_value DECIMAL(10,2)
);
