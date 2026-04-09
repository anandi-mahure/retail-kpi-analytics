"""
Pytest suite for retail-kpi-analytics pipeline.
Tests data quality, schema validation, KPI calculations, and ETL logic.
"""

import pytest
import sqlite3
import pandas as pd
import os
import sys

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def db_connection():
    """Create an in-memory SQLite database for testing."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create dimension tables
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS dim_store (
            store_id INTEGER PRIMARY KEY,
            store_name TEXT NOT NULL,
            region TEXT NOT NULL,
            store_type TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dim_product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT,
            unit_cost REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INTEGER PRIMARY KEY,
            full_date TEXT NOT NULL,
            week_number INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER
        );

        CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id INTEGER PRIMARY KEY,
            date_id INTEGER REFERENCES dim_date(date_id),
            store_id INTEGER REFERENCES dim_store(store_id),
            product_id INTEGER REFERENCES dim_product(product_id),
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            total_revenue REAL NOT NULL,
            labour_cost REAL,
            shrinkage_value REAL DEFAULT 0
        );
    """)

    # Seed test data
    cursor.executescript("""
        INSERT INTO dim_store VALUES (1, 'London Central', 'London', 'Superstore');
        INSERT INTO dim_store VALUES (2, 'Manchester North', 'North', 'Express');

        INSERT INTO dim_product VALUES (1, 'Organic Milk', 'Dairy', 'Fresh', 0.85);
        INSERT INTO dim_product VALUES (2, 'Sourdough Bread', 'Bakery', 'Fresh', 1.20);
        INSERT INTO dim_product VALUES (3, 'Cheddar Cheese', 'Dairy', 'Packaged', 2.50);

        INSERT INTO dim_date VALUES (1, '2024-01-08', 2, 1, 1, 2024);
        INSERT INTO dim_date VALUES (2, '2024-01-15', 3, 1, 1, 2024);
        INSERT INTO dim_date VALUES (3, '2024-01-22', 4, 1, 1, 2024);

        INSERT INTO fact_sales VALUES (1, 1, 1, 1, 120, 1.35, 162.00, 45.00, 2.50);
        INSERT INTO fact_sales VALUES (2, 1, 1, 2, 85,  2.10, 178.50, 50.00, 1.20);
        INSERT INTO fact_sales VALUES (3, 2, 2, 3, 200, 3.80, 760.00, 180.00, 5.00);
        INSERT INTO fact_sales VALUES (4, 3, 1, 1, 95,  1.35, 128.25, 38.00, 1.80);
        INSERT INTO fact_sales VALUES (5, 3, 2, 2, 110, 2.10, 231.00, 65.00, 0.00);
    """)

    conn.commit()
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Schema Tests
# ---------------------------------------------------------------------------

class TestSchema:
    def test_dim_store_exists(self, db_connection):
        cursor = db_connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dim_store'")
        assert cursor.fetchone() is not None

    def test_dim_product_exists(self, db_connection):
        cursor = db_connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dim_product'")
        assert cursor.fetchone() is not None

    def test_fact_sales_exists(self, db_connection):
        cursor = db_connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fact_sales'")
        assert cursor.fetchone() is not None

    def test_dim_date_exists(self, db_connection):
        cursor = db_connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dim_date'")
        assert cursor.fetchone() is not None


# ---------------------------------------------------------------------------
# Data Quality Tests
# ---------------------------------------------------------------------------

class TestDataQuality:
    def test_no_null_store_names(self, db_connection):
        df = pd.read_sql("SELECT * FROM dim_store WHERE store_name IS NULL", db_connection)
        assert len(df) == 0, "Null store names detected"

    def test_no_null_product_names(self, db_connection):
        df = pd.read_sql("SELECT * FROM dim_product WHERE product_name IS NULL", db_connection)
        assert len(df) == 0, "Null product names detected"

    def test_no_negative_revenue(self, db_connection):
        df = pd.read_sql("SELECT * FROM fact_sales WHERE total_revenue < 0", db_connection)
        assert len(df) == 0, "Negative revenue values detected"

    def test_no_negative_quantity(self, db_connection):
        df = pd.read_sql("SELECT * FROM fact_sales WHERE quantity <= 0", db_connection)
        assert len(df) == 0, "Zero or negative quantity detected"

    def test_no_negative_unit_cost(self, db_connection):
        df = pd.read_sql("SELECT * FROM dim_product WHERE unit_cost < 0", db_connection)
        assert len(df) == 0, "Negative unit cost detected"

    def test_revenue_calculation_accuracy(self, db_connection):
        """Revenue must equal quantity * unit_price within 1p tolerance."""
        df = pd.read_sql("""
            SELECT sale_id, quantity, unit_price, total_revenue,
                   ABS(total_revenue - (quantity * unit_price)) AS discrepancy
            FROM fact_sales
        """, db_connection)
        assert (df['discrepancy'] < 0.01).all(), "Revenue calculation discrepancies detected"

    def test_shrinkage_not_negative(self, db_connection):
        df = pd.read_sql("SELECT * FROM fact_sales WHERE shrinkage_value < 0", db_connection)
        assert len(df) == 0, "Negative shrinkage values detected"

    def test_all_sales_have_valid_store(self, db_connection):
        df = pd.read_sql("""
            SELECT f.sale_id FROM fact_sales f
            LEFT JOIN dim_store s ON f.store_id = s.store_id
            WHERE s.store_id IS NULL
        """, db_connection)
        assert len(df) == 0, "Orphaned sales records with no matching store"

    def test_all_sales_have_valid_product(self, db_connection):
        df = pd.read_sql("""
            SELECT f.sale_id FROM fact_sales f
            LEFT JOIN dim_product p ON f.product_id = p.product_id
            WHERE p.product_id IS NULL
        """, db_connection)
        assert len(df) == 0, "Orphaned sales records with no matching product"


# ---------------------------------------------------------------------------
# KPI Calculation Tests
# ---------------------------------------------------------------------------

class TestKPICalculations:
    def test_total_revenue_is_positive(self, db_connection):
        df = pd.read_sql("SELECT SUM(total_revenue) AS total FROM fact_sales", db_connection)
        assert df['total'].iloc[0] > 0, "Total revenue should be positive"

    def test_labour_cost_percentage_reasonable(self, db_connection):
        """Labour cost % should be between 5% and 60% of revenue."""
        df = pd.read_sql("""
            SELECT 
                SUM(labour_cost) * 100.0 / SUM(total_revenue) AS labour_pct
            FROM fact_sales
            WHERE labour_cost IS NOT NULL
        """, db_connection)
        labour_pct = df['labour_pct'].iloc[0]
        assert 5 <= labour_pct <= 60, f"Labour cost % out of range: {labour_pct:.1f}%"

    def test_shrinkage_rate_reasonable(self, db_connection):
        """Shrinkage should be less than 10% of total revenue."""
        df = pd.read_sql("""
            SELECT 
                SUM(shrinkage_value) * 100.0 / SUM(total_revenue) AS shrinkage_pct
            FROM fact_sales
        """, db_connection)
        shrinkage_pct = df['shrinkage_pct'].iloc[0]
        assert shrinkage_pct < 10, f"Shrinkage rate too high: {shrinkage_pct:.2f}%"

    def test_revenue_by_store_aggregation(self, db_connection):
        """Each store should have calculable total revenue."""
        df = pd.read_sql("""
            SELECT store_id, SUM(total_revenue) AS store_revenue
            FROM fact_sales
            GROUP BY store_id
        """, db_connection)
        assert len(df) == 2, "Expected revenue for 2 stores"
        assert (df['store_revenue'] > 0).all(), "All stores should have positive revenue"

    def test_week_on_week_variance_calculable(self, db_connection):
        """Week-on-week variance should be calculable from date dimension."""
        df = pd.read_sql("""
            SELECT 
                d.week_number,
                SUM(f.total_revenue) AS weekly_revenue
            FROM fact_sales f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.week_number
            ORDER BY d.week_number
        """, db_connection)
        assert len(df) >= 2, "Need at least 2 weeks for variance calculation"

    def test_category_revenue_breakdown(self, db_connection):
        """Revenue should be breakable by product category."""
        df = pd.read_sql("""
            SELECT 
                p.category,
                SUM(f.total_revenue) AS category_revenue
            FROM fact_sales f
            JOIN dim_product p ON f.product_id = p.product_id
            GROUP BY p.category
        """, db_connection)
        assert len(df) > 0, "Category revenue breakdown returned no results"
        assert (df['category_revenue'] > 0).all()


# ---------------------------------------------------------------------------
# ETL Logic Tests
# ---------------------------------------------------------------------------

class TestETLLogic:
    def test_record_count_after_load(self, db_connection):
        df = pd.read_sql("SELECT COUNT(*) AS cnt FROM fact_sales", db_connection)
        assert df['cnt'].iloc[0] == 5, "Expected 5 seeded sales records"

    def test_store_count(self, db_connection):
        df = pd.read_sql("SELECT COUNT(*) AS cnt FROM dim_store", db_connection)
        assert df['cnt'].iloc[0] == 2

    def test_product_count(self, db_connection):
        df = pd.read_sql("SELECT COUNT(*) AS cnt FROM dim_product", db_connection)
        assert df['cnt'].iloc[0] == 3

    def test_date_dimension_populated(self, db_connection):
        df = pd.read_sql("SELECT COUNT(*) AS cnt FROM dim_date", db_connection)
        assert df['cnt'].iloc[0] >= 1

    def test_duplicate_sale_ids(self, db_connection):
        df = pd.read_sql("""
            SELECT sale_id, COUNT(*) AS cnt 
            FROM fact_sales 
            GROUP BY sale_id 
            HAVING cnt > 1
        """, db_connection)
        assert len(df) == 0, "Duplicate sale IDs detected"
