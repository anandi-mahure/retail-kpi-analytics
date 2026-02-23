-- ============================================================
-- RETAIL KPI ANALYTICS — DATA QUALITY VALIDATION
-- Script: 02_data_quality_validation.sql
-- Purpose: Automated DQ checks before BI consumption
-- Run BEFORE any KPI calculations — bad data in = bad KPIs out
-- ============================================================

-- ── CHECK 1: NULL checks on critical columns ─────────────────
-- Any nulls in these columns break KPI calculations downstream

SELECT 
    'fact_transactions'     AS table_name,
    'date_id'               AS column_name,
    COUNT(*)                AS null_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_transactions), 2) AS null_pct
FROM fact_transactions WHERE date_id IS NULL

UNION ALL

SELECT 'fact_transactions', 'store_id',
    COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_transactions), 2)
FROM fact_transactions WHERE store_id IS NULL

UNION ALL

SELECT 'fact_transactions', 'unit_price',
    COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_transactions), 2)
FROM fact_transactions WHERE unit_price IS NULL

UNION ALL

SELECT 'fact_labour', 'total_labour_cost',
    COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_labour), 2)
FROM fact_labour WHERE total_labour_cost IS NULL;

-- Expected result: null_count = 0 for all rows
-- If any null_count > 0: investigate source data before proceeding


-- ── CHECK 2: Orphan records (referential integrity) ──────────
-- These are transactions pointing to stores or products that don't exist
-- Classic sign of ETL joining wrong keys or data loaded out of order

SELECT 
    'Orphan: transaction → store' AS issue_type,
    COUNT(*) AS record_count
FROM fact_transactions ft
LEFT JOIN dim_store ds ON ft.store_id = ds.store_id
WHERE ds.store_id IS NULL

UNION ALL

SELECT 
    'Orphan: transaction → product',
    COUNT(*)
FROM fact_transactions ft
LEFT JOIN dim_product dp ON ft.product_id = dp.product_id
WHERE dp.product_id IS NULL

UNION ALL

SELECT 
    'Orphan: transaction → date',
    COUNT(*)
FROM fact_transactions ft
LEFT JOIN dim_date dd ON ft.date_id = dd.date_id
WHERE dd.date_id IS NULL

UNION ALL

SELECT 
    'Orphan: shrinkage → store',
    COUNT(*)
FROM fact_shrinkage fs
LEFT JOIN dim_store ds ON fs.store_id = ds.store_id
WHERE ds.store_id IS NULL;

-- Expected: all counts = 0
-- If not: do NOT load to Power BI until resolved


-- ── CHECK 3: Duplicate detection ─────────────────────────────
-- Duplicates inflate every KPI — sales appear doubled, labour doubled

SELECT 
    'Duplicate transaction_ids' AS issue,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_count
FROM fact_transactions

UNION ALL

SELECT 
    'Duplicate labour records (same store+date)',
    COUNT(*) - COUNT(DISTINCT store_id || '|' || date_id)
FROM fact_labour

UNION ALL

SELECT
    'Duplicate date_ids in dim_date',
    COUNT(*) - COUNT(DISTINCT date_id)
FROM dim_date;

-- Expected: all 0
-- If fact_labour shows duplicates: two records for same store/date = double-counting labour cost


-- ── CHECK 4: Impossible values ────────────────────────────────
-- These catch data entry errors and ETL bugs
-- Zero or negative sales, negative labour cost, future dates

SELECT 'Zero or negative unit_price' AS issue, COUNT(*) AS count
FROM fact_transactions WHERE unit_price <= 0

UNION ALL

SELECT 'Zero or negative quantity', COUNT(*)
FROM fact_transactions WHERE quantity <= 0

UNION ALL

SELECT 'Negative labour cost', COUNT(*)
FROM fact_labour WHERE total_labour_cost < 0

UNION ALL

SELECT 'Future-dated transactions', COUNT(*)
FROM fact_transactions ft
JOIN dim_date dd ON ft.date_id = dd.date_id
WHERE dd.full_date > CURRENT_DATE

UNION ALL

SELECT 'Net sales > £10,000 per transaction (outlier check)', COUNT(*)
FROM fact_transactions 
WHERE quantity * unit_price > 10000;

-- Expected: all 0
-- The >£10,000 check may have legitimate exceptions for bulk orders


-- ── CHECK 5: Date range completeness ─────────────────────────
-- Are there gaps in trading days? Missing days = missing KPIs

WITH date_gaps AS (
    SELECT 
        full_date,
        LAG(full_date) OVER (ORDER BY full_date) AS prev_date,
        full_date - LAG(full_date) OVER (ORDER BY full_date) AS gap_days
    FROM dim_date
    WHERE is_weekend = FALSE AND is_bank_holiday = FALSE
)
SELECT 
    full_date AS gap_after_this_date,
    prev_date,
    gap_days
FROM date_gaps
WHERE gap_days > 1
ORDER BY full_date;

-- Expected: no rows returned (no gaps in trading days)


-- ── CHECK 6: Store coverage completeness ─────────────────────
-- Every active store should have transactions every trading day

SELECT 
    ds.store_id,
    ds.store_name,
    COUNT(DISTINCT ft.date_id) AS days_with_transactions,
    (SELECT COUNT(*) FROM dim_date WHERE is_weekend = FALSE) AS total_trading_days,
    ROUND(COUNT(DISTINCT ft.date_id) * 100.0 / 
          (SELECT COUNT(*) FROM dim_date WHERE is_weekend = FALSE), 1) AS coverage_pct
FROM dim_store ds
LEFT JOIN fact_transactions ft ON ds.store_id = ft.store_id
GROUP BY ds.store_id, ds.store_name
ORDER BY coverage_pct ASC;

-- Any store below 90% coverage needs investigation
-- Low coverage = either store was closed or data feed failed


-- ── CHECK 7: Labour vs sales sanity check ────────────────────
-- Labour cost % should be between 8% and 18% in UK retail
-- Outside this range usually means a data issue, not a real performance issue

WITH daily_kpis AS (
    SELECT 
        ft.date_id,
        ft.store_id,
        SUM(ft.quantity * ft.unit_price) AS net_sales,
        SUM(fl.total_labour_cost)         AS labour_cost
    FROM fact_transactions ft
    LEFT JOIN fact_labour fl 
        ON ft.date_id = fl.date_id AND ft.store_id = fl.store_id
    GROUP BY ft.date_id, ft.store_id
)
SELECT 
    date_id,
    store_id,
    ROUND(net_sales, 2)                                      AS net_sales,
    ROUND(labour_cost, 2)                                    AS labour_cost,
    ROUND(labour_cost / NULLIF(net_sales, 0) * 100, 2)      AS labour_pct,
    CASE 
        WHEN labour_cost / NULLIF(net_sales, 0) > 0.25 
            THEN 'INVESTIGATE: labour cost > 25% of sales'
        WHEN labour_cost / NULLIF(net_sales, 0) < 0.03 
            THEN 'INVESTIGATE: labour cost < 3% of sales'
        ELSE 'OK'
    END AS dq_flag
FROM daily_kpis
WHERE labour_cost / NULLIF(net_sales, 0) > 0.25
   OR labour_cost / NULLIF(net_sales, 0) < 0.03
ORDER BY labour_pct DESC;

-- Expected: 0 rows (no extreme outliers)
-- If rows returned: investigate those store/date combinations before reporting


-- ── DQ SUMMARY DASHBOARD ─────────────────────────────────────
-- Run this last — gives you a single pass/fail view

SELECT 
    'Total transactions loaded'     AS metric, 
    COUNT(*)                        AS value 
FROM fact_transactions

UNION ALL SELECT 'Distinct stores',         COUNT(DISTINCT store_id) FROM fact_transactions
UNION ALL SELECT 'Distinct products',       COUNT(DISTINCT product_id) FROM fact_transactions
UNION ALL SELECT 'Date range (days)',        COUNT(DISTINCT date_id) FROM fact_transactions
UNION ALL SELECT 'Total net sales (£)',     ROUND(SUM(quantity * unit_price), 2) FROM fact_transactions
UNION ALL SELECT 'Avg daily sales (£)',     ROUND(AVG(daily_sales), 2) 
    FROM (SELECT date_id, SUM(quantity * unit_price) AS daily_sales FROM fact_transactions GROUP BY date_id)
UNION ALL SELECT 'Total shrinkage (£)',     ROUND(SUM(shrinkage_value), 2) FROM fact_shrinkage
UNION ALL SELECT 'Total labour cost (£)',   ROUND(SUM(total_labour_cost), 2) FROM fact_labour;
