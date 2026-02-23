--- ============================================================
-- RETAIL KPI ANALYTICS — KPI CALCULATIONS
-- Script: 03_kpi_labour_cost.sql
-- Purpose: Core retail KPIs — sales, labour, availability
-- ============================================================

-- ── KPI 1: Weekly net sales by store ─────────────────────────
SELECT 
    dd.fiscal_week,
    dd.year,
    ds.store_name,
    ds.region,
    SUM(ft.quantity * ft.unit_price)    AS net_sales,
    COUNT(DISTINCT ft.date_id)          AS trading_days,
    ROUND(SUM(ft.quantity * ft.unit_price) / COUNT(DISTINCT ft.date_id), 2) AS avg_daily_sales
FROM fact_transactions ft
JOIN dim_date  dd ON ft.date_id  = dd.date_id
JOIN dim_store ds ON ft.store_id = ds.store_id
GROUP BY dd.fiscal_week, dd.year, ds.store_name, ds.region
ORDER BY dd.year, dd.fiscal_week, net_sales DESC;


-- ── KPI 2: Labour cost % by store by week ────────────────────
-- UK benchmark: 8–14% labour cost. Above 14% = overspend alert.

WITH weekly_sales AS (
    SELECT 
        dd.fiscal_week,
        dd.year,
        ft.store_id,
        SUM(ft.quantity * ft.unit_price) AS net_sales
    FROM fact_transactions ft
    JOIN dim_date dd ON ft.date_id = dd.date_id
    GROUP BY dd.fiscal_week, dd.year, ft.store_id
),
weekly_labour AS (
    SELECT 
        dd.fiscal_week,
        dd.year,
        fl.store_id,
        SUM(fl.total_labour_cost)       AS labour_cost,
        SUM(fl.hours_worked)            AS hours_worked
    FROM fact_labour fl
    JOIN dim_date dd ON fl.date_id = dd.date_id
    GROUP BY dd.fiscal_week, dd.year, fl.store_id
)
SELECT 
    ws.fiscal_week,
    ws.year,
    ds.store_name,
    ds.region,
    ROUND(ws.net_sales, 2)              AS net_sales,
    ROUND(wl.labour_cost, 2)            AS labour_cost,
    ROUND(wl.labour_cost / NULLIF(ws.net_sales, 0) * 100, 2) AS labour_cost_pct,
    ROUND(wl.hours_worked, 1)           AS hours_worked,
    ROUND(ws.net_sales / NULLIF(wl.hours_worked, 0), 2) AS sales_per_hour,
    CASE 
        WHEN wl.labour_cost / NULLIF(ws.net_sales, 0) > 0.14 THEN '🔴 OVERSPEND'
        WHEN wl.labour_cost / NULLIF(ws.net_sales, 0) < 0.08 THEN '🟡 UNDERSTAFFED'
        ELSE '🟢 ON TARGET'
    END AS labour_status
FROM weekly_sales ws
JOIN weekly_labour wl 
    ON ws.fiscal_week = wl.fiscal_week 
    AND ws.year = wl.year 
    AND ws.store_id = wl.store_id
JOIN dim_store ds ON ws.store_id = ds.store_id
ORDER BY ws.year, ws.fiscal_week, labour_cost_pct DESC;


-- ── KPI 3: Week-on-week sales variance ───────────────────────
-- Using window functions to compare each week to the prior week

WITH weekly_store_sales AS (
    SELECT 
        dd.fiscal_week,
        dd.year,
        ft.store_id,
        ds.store_name,
        SUM(ft.quantity * ft.unit_price) AS net_sales
    FROM fact_transactions ft
    JOIN dim_date  dd ON ft.date_id  = dd.date_id
    JOIN dim_store ds ON ft.store_id = ds.store_id
    GROUP BY dd.fiscal_week, dd.year, ft.store_id, ds.store_name
)
SELECT 
    fiscal_week,
    year,
    store_name,
    ROUND(net_sales, 2)                                         AS net_sales,
    ROUND(LAG(net_sales) OVER 
        (PARTITION BY store_id ORDER BY year, fiscal_week), 2) AS prior_week_sales,
    ROUND(net_sales - LAG(net_sales) OVER 
        (PARTITION BY store_id ORDER BY year, fiscal_week), 2) AS variance_£,
    ROUND((net_sales - LAG(net_sales) OVER 
        (PARTITION BY store_id ORDER BY year, fiscal_week)) 
        / NULLIF(LAG(net_sales) OVER 
        (PARTITION BY store_id ORDER BY year, fiscal_week), 0) * 100, 1) AS variance_pct
FROM weekly_store_sales
ORDER BY year, fiscal_week, store_name;


-- ── KPI 4: Sales by category and region ──────────────────────
SELECT 
    ds.region,
    dp.category,
    dd.month_name,
    dd.year,
    SUM(ft.quantity * ft.unit_price)    AS net_sales,
    SUM(ft.quantity)                    AS units_sold,
    COUNT(DISTINCT ft.transaction_id)   AS transactions,
    ROUND(SUM(ft.quantity * ft.unit_price) / COUNT(DISTINCT ft.transaction_id), 2) AS avg_basket_value
FROM fact_transactions ft
JOIN dim_date    dd ON ft.date_id    = dd.date_id
JOIN dim_store   ds ON ft.store_id   = ds.store_id
JOIN dim_product dp ON ft.product_id = dp.product_id
GROUP BY ds.region, dp.category, dd.month_name, dd.year, dd.month_number
ORDER BY dd.year, dd.month_number, ds.region, net_sales DESC;


-- ── KPI 5: Top 20 products by net sales ──────────────────────
SELECT 
    dp.product_name,
    dp.category,
    dp.subcategory,
    SUM(ft.quantity)                        AS total_units,
    ROUND(SUM(ft.quantity * ft.unit_price), 2) AS total_sales,
    RANK() OVER (ORDER BY SUM(ft.quantity * ft.unit_price) DESC) AS sales_rank,
    ROUND(SUM(ft.quantity * ft.unit_price) / 
        (SELECT SUM(quantity * unit_price) FROM fact_transactions) * 100, 2) AS pct_of_total_sales
FROM fact_transactions ft
JOIN dim_product dp ON ft.product_id = dp.product_id
GROUP BY dp.product_name, dp.category, dp.subcategory
ORDER BY total_sales DESC
LIMIT 20;
