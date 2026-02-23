-- ============================================================
-- RETAIL KPI ANALYTICS — SHRINKAGE ANOMALY DETECTION
-- Script: 04_shrinkage_anomaly_detection.sql
-- Purpose: Z-score anomaly detection on per-store rolling baseline
-- KEY DESIGN: Uses store's own 12-week history, NOT national average
-- Reason: A London store and a Welsh store have structurally 
--         different shrinkage patterns — same benchmark = wrong alerts
-- ============================================================

-- ── STEP 1: Weekly shrinkage by store ────────────────────────
WITH weekly_shrinkage AS (
    SELECT 
        dd.fiscal_week,
        dd.year,
        fs.store_id,
        ds.store_name,
        ds.region,
        SUM(fs.shrinkage_value)         AS shrinkage_value,
        SUM(fs.shrinkage_units)         AS shrinkage_units,
        COUNT(*)                        AS incident_count
    FROM fact_shrinkage fs
    JOIN dim_date  dd ON fs.date_id  = dd.date_id
    JOIN dim_store ds ON fs.store_id = ds.store_id
    GROUP BY dd.fiscal_week, dd.year, fs.store_id, ds.store_name, ds.region
),

-- ── STEP 2: Calculate 12-week rolling baseline per store ─────
-- This is the key — each store compared to its OWN recent history

rolling_baseline AS (
    SELECT 
        fiscal_week,
        year,
        store_id,
        store_name,
        region,
        shrinkage_value,
        AVG(shrinkage_value) OVER (
            PARTITION BY store_id 
            ORDER BY year, fiscal_week
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_12w,
        STDDEV(shrinkage_value) OVER (
            PARTITION BY store_id 
            ORDER BY year, fiscal_week
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS rolling_stddev_12w,
        COUNT(shrinkage_value) OVER (
            PARTITION BY store_id
            ORDER BY year, fiscal_week
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS weeks_of_history
    FROM weekly_shrinkage
),

-- ── STEP 3: Calculate Z-score ────────────────────────────────
-- Z-score = how many standard deviations from the store's own average
-- Formula: (current_value - rolling_average) / standard_deviation
-- Z > 2.0 = unusual (top 2.5% of distribution)
-- Z > 2.5 = HIGH ALERT
-- Z > 3.0 = CRITICAL (less than 1 in 1000 probability if normal)

z_scores AS (
    SELECT 
        fiscal_week,
        year,
        store_id,
        store_name,
        region,
        ROUND(shrinkage_value, 2)       AS shrinkage_value,
        ROUND(rolling_avg_12w, 2)       AS baseline_avg,
        ROUND(rolling_stddev_12w, 2)    AS baseline_stddev,
        weeks_of_history,
        ROUND(
            (shrinkage_value - rolling_avg_12w) 
            / NULLIF(rolling_stddev_12w, 0), 
        2) AS z_score
    FROM rolling_baseline
    WHERE weeks_of_history >= 4  -- Need at least 4 weeks of history for meaningful baseline
)

-- ── STEP 4: Flag anomalies with severity ─────────────────────
SELECT 
    fiscal_week,
    year,
    store_name,
    region,
    shrinkage_value                         AS this_week_£,
    baseline_avg                            AS normal_£,
    ROUND(shrinkage_value - baseline_avg, 2) AS variance_vs_baseline_£,
    z_score,
    weeks_of_history                        AS baseline_weeks,
    CASE 
        WHEN z_score >= 3.0 THEN '🔴 CRITICAL — Investigate immediately'
        WHEN z_score >= 2.5 THEN '🟠 HIGH — Review this week'
        WHEN z_score >= 2.0 THEN '🟡 ELEVATED — Monitor closely'
        WHEN z_score <= -2.0 THEN '🟢 UNUSUALLY LOW — Verify data'
        ELSE 'Normal'
    END AS alert_level
FROM z_scores
WHERE z_score >= 2.0 OR z_score <= -2.0  -- Only show anomalies
ORDER BY z_score DESC;


-- ── BONUS: Shrinkage by cause code ───────────────────────────
-- Identifies whether your problem is theft, waste, damage, or admin error

SELECT 
    ds.region,
    fs.cause_code,
    dd.month_name,
    dd.year,
    COUNT(*)                            AS incident_count,
    ROUND(SUM(fs.shrinkage_value), 2)   AS total_shrinkage_£,
    ROUND(AVG(fs.shrinkage_value), 2)   AS avg_per_incident_£,
    RANK() OVER (
        PARTITION BY ds.region 
        ORDER BY SUM(fs.shrinkage_value) DESC
    ) AS rank_by_region
FROM fact_shrinkage fs
JOIN dim_date  dd ON fs.date_id  = dd.date_id
JOIN dim_store ds ON fs.store_id = ds.store_id
GROUP BY ds.region, fs.cause_code, dd.month_name, dd.year, dd.month_number
ORDER BY dd.year, dd.month_number, total_shrinkage_£ DESC;


-- ── BONUS: Shrinkage as % of sales ───────────────────────────
-- Industry acceptable rate: below 1.5% of sales

WITH shrink_by_week AS (
    SELECT date_id, store_id, SUM(shrinkage_value) AS shrinkage
    FROM fact_shrinkage GROUP BY date_id, store_id
),
sales_by_week AS (
    SELECT date_id, store_id, SUM(quantity * unit_price) AS sales
    FROM fact_transactions GROUP BY date_id, store_id
)
SELECT 
    ds.store_name,
    ds.region,
    dd.fiscal_week,
    ROUND(sw.sales, 2)                  AS net_sales,
    ROUND(sh.shrinkage, 2)              AS shrinkage_£,
    ROUND(sh.shrinkage / NULLIF(sw.sales, 0) * 100, 3) AS shrinkage_pct,
    CASE 
        WHEN sh.shrinkage / NULLIF(sw.sales, 0) > 0.015 
            THEN '⚠️ ABOVE INDUSTRY THRESHOLD'
        ELSE '✓ Within benchmark'
    END AS benchmark_status
FROM sales_by_week sw
JOIN shrink_by_week sh 
    ON sw.date_id = sh.date_id AND sw.store_id = sh.store_id
JOIN dim_store ds ON sw.store_id = ds.store_id
JOIN dim_date  dd ON sw.date_id  = dd.date_id
ORDER BY shrinkage_pct DESC;
