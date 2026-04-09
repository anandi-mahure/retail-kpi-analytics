# retail-kpi-analytics

![CI](https://github.com/anandi-mahure/erp-reconciliation-pipeline/actions/workflows/ci.yml/badge.svg)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-SQLite-4479A1?style=flat-square&logo=postgresql&logoColor=white)](#)
[![Power BI](https://img.shields.io/badge/Power_BI-DAX%20%7C%20RLS-F2C811?style=flat-square&logo=powerbi&logoColor=black)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)
[![pytest](https://img.shields.io/badge/Tests-pytest-blue?style=flat-square&logo=pytest&logoColor=white)](#)
[![Records](https://img.shields.io/badge/Records-715%2C536-orange?style=flat-square)](#)

End-to-end retail analytics system — SQL ETL pipeline, Bronze-Silver-Gold architecture, star schema data model, and Power BI KPI dashboard with DAX, RLS, and deployment pipeline. Processes **715,536 transactional records** across 10 stores, 42 products, and 2 years of trading data.

> **Business Impact:** Reporting preparation time reduced from 45 minutes to under 5 minutes. Automated shrinkage anomaly detection flagged 7 critical incidents including a Z-score 6.68 theft spike at Manchester Central — £33,140 above baseline.

---

## Table of Contents

- [Business Context](#business-context)
- [Architecture](#architecture)
- [Key Business Questions Answered](#key-business-questions-answered)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [SQL Modules](#sql-modules)
- [Key Findings](#key-findings)
- [Setup and Usage](#setup-and-usage)
- [Test Suite](#test-suite)
- [Skills Demonstrated](#skills-demonstrated)

---

## Business Context

Retail operations generate high-volume transactional data across stores, products, and time periods. Without structured analytics infrastructure, reporting is manual, inconsistent, and slow — leaving managers without the insight to act on labour overspend or loss events in time.

This system builds a complete analytics layer on top of raw retail data:

- **ETL pipeline** generates and validates 715K+ transactions across 10 UK stores
- **Bronze-Silver-Gold architecture** mirrors enterprise data warehouse patterns
- **SQL KPI layer** calculates weekly sales, labour cost %, week-on-week variance, and Z-score shrinkage anomaly detection
- **Power BI dashboard** delivers self-serve reporting with row-level security by region

---

## Architecture

```
Synthetic Data Generator (Python)
        │
        ▼
CSV Files → data/generated/
        │
        ▼
ETL Loader (load_to_database.py → SQLAlchemy)
        │
        ▼
SQLite Database — Bronze Schema
├── fact_transactions   (715,536 rows)
├── fact_labour         (7,310 rows)
├── fact_shrinkage      (5,697 rows)
├── dim_date            (731 rows)
├── dim_store           (10 rows)
└── dim_product         (42 rows)
        │
        ▼
SQL Analytics Layer
├── 01_create_schema.sql              — Bronze/Silver/Gold DDL
├── 02_data_quality_validation.sql    — 7 automated DQ checks
├── 03_kpi_labour_cost.sql            — Labour cost %, WoW variance, basket value
└── 04_shrinkage_anomaly_detection.sql — Z-score rolling baseline detection
        │
        ▼
Power BI Dashboard
├── DAX measures (time intelligence, YTD, WoW variance)
├── Row-Level Security (region-based)
└── Deployment pipeline configuration
```

---

## Key Business Questions Answered

| # | Business Question | Module |
|---|---|---|
| 1 | What are weekly net sales by store and region? | `03_kpi_labour_cost.sql` — KPI 1 |
| 2 | Which stores have labour cost % above the 14% threshold? | `03_kpi_labour_cost.sql` — KPI 2 |
| 3 | What is the week-on-week revenue variance per store? | `03_kpi_labour_cost.sql` — KPI 3 |
| 4 | Which categories and regions drive the most sales? | `03_kpi_labour_cost.sql` — KPI 4 |
| 5 | What are the top 20 products by net sales? | `03_kpi_labour_cost.sql` — KPI 5 |
| 6 | Which stores show Z-score shrinkage anomalies? | `04_shrinkage_anomaly_detection.sql` |
| 7 | What is shrinkage by cause code (theft/waste/damage)? | `04_shrinkage_anomaly_detection.sql` |
| 8 | Are there data quality issues before BI consumption? | `02_data_quality_validation.sql` |

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Data Generation** | Python, NumPy, Faker — realistic seasonal patterns, UK bank holidays |
| **ETL Pipeline** | Python, Pandas, SQLAlchemy |
| **Database** | SQLite (dev) with Bronze/Silver/Gold schema pattern |
| **Data Modelling** | Star schema — 3 fact tables + 3 dimension tables |
| **SQL Analytics** | CTEs, Window Functions (LAG, RANK, STDDEV), Z-score anomaly detection |
| **BI & Reporting** | Power BI — DAX, Row-Level Security, Deployment Pipeline |
| **Testing** | pytest — schema, data quality, KPI, ETL |
| **CI/CD** | GitHub Actions |

---

## Project Structure

```
retail-kpi-analytics/
├── .github/
│   └── workflows/
│       └── ci.yml                         # CI — pytest on every push
├── architecture/
│   └── pipeline_diagram.png               # System architecture diagram
├── data/
│   └── generated/                         # CSV outputs from data generator
├── docs/                                  # Supporting documentation
├── powerbi/
│   └── screenshots/                       # Dashboard screenshots
├── python/
│   ├── generate_synthetic_data.py         # 715K+ row retail data generator
│   └── load_to_database.py               # ETL loader — CSV to SQLite
├── sql/
│   ├── 01_create_schema.sql               # Bronze/Silver/Gold DDL
│   ├── 02_data_quality_validation.sql     # 7 automated DQ checks
│   ├── 03_kpi_labour_cost.sql             # Core KPI calculations
│   └── 04_shrinkage_anomaly_detection.sql # Z-score anomaly detection
├── tests/
│   └── test_retail_pipeline.py            # pytest suite
├── .gitignore
├── CONTRIBUTING.md
├── LICENSE
├── README.md
└── requirements.txt
```

---

## Data Model

| Table | Rows | Description |
|---|---|---|
| `fact_transactions` | 715,536 | Every retail transaction — store, product, quantity, price |
| `fact_labour` | 7,310 | Daily labour hours and cost per store |
| `fact_shrinkage` | 5,697 | Weekly shrinkage incidents by cause code |
| `dim_date` | 731 | Full date dimension with fiscal weeks, bank holidays, weekends |
| `dim_store` | 10 | 10 UK stores across 5 regions (London, North, Midlands, Wales, South) |
| `dim_product` | 42 | Products across 5 categories (Ambient, Fresh, Frozen, BWS, Non-Food) |

**Total sales value: £5,422,618 across 2024–2025**
**Average basket value: £7.58**

---

## SQL Modules

### `01_create_schema.sql`
Defines Bronze, Silver, and Gold schemas mirroring enterprise data warehouse architecture. Bronze holds raw ingested data, Silver holds validated data, Gold holds the star schema for Power BI consumption.

### `02_data_quality_validation.sql`
Seven automated DQ checks run before any KPI calculations:
- NULL checks on critical columns with percentage reporting
- Orphan record detection across all foreign keys
- Duplicate detection on transaction IDs and labour records
- Impossible value detection (negative prices, future dates, outliers)
- Date range completeness — gaps in trading days
- Store coverage completeness — flags stores below 90% trading day coverage
- Labour vs sales sanity check — flags labour cost outside 3–25% band

### `03_kpi_labour_cost.sql`
Five core KPI queries using CTEs and window functions:
- **KPI 1:** Weekly net sales by store with average daily sales
- **KPI 2:** Labour cost % by store with RAG status (target: 8–14%)
- **KPI 3:** Week-on-week variance using `LAG()` window function
- **KPI 4:** Sales by category and region with average basket value
- **KPI 5:** Top 20 products by net sales with % of total contribution

### `04_shrinkage_anomaly_detection.sql`
Statistical anomaly detection using a **12-week rolling per-store baseline** — not a national average. Design rationale: London and Welsh stores have structurally different shrinkage patterns, so a shared benchmark produces false alerts.

Z-score thresholds:
- Z ≥ 3.0 → CRITICAL — Investigate immediately
- Z ≥ 2.5 → HIGH ALERT — Review this week
- Z ≥ 2.0 → ELEVATED — Monitor closely

---

## Key Findings

| Metric | Result |
|---|---|
| Total transactions | 715,536 |
| Total sales value | £5,422,618 |
| Average basket value | £7.58 |
| Stores monitored | 10 across 5 UK regions |
| Trading days | 731 (2024–2025 including weekends) |
| Shrinkage anomalies detected | 7 (5 CRITICAL, 1 HIGH, 1 ELEVATED) |
| Highest Z-score | 6.68 — Manchester Central fiscal week 20 (£33,140 above baseline) |
| Labour overspend | Southampton 19.45%, Birmingham Moseley 19.01% in week 8 |

---

## Setup and Usage

### Prerequisites
- Python 3.10+

### Installation

```bash
git clone https://github.com/anandi-mahure/retail-kpi-analytics.git
cd retail-kpi-analytics
pip install -r requirements.txt
```

### Run the pipeline

```bash
# Step 1 — Generate 715K+ synthetic retail transactions
python python/generate_synthetic_data.py

# Step 2 — Load to SQLite database
python python/load_to_database.py
```

### Run SQL analytics
Open `data/retail_kpi.db` in any SQLite client (DB Browser, DBeaver) and execute scripts in order:
```
01_create_schema.sql
02_data_quality_validation.sql
03_kpi_labour_cost.sql
04_shrinkage_anomaly_detection.sql
```

---

## Test Suite

```bash
pytest tests/ -v
pytest tests/ -v --cov=python --cov-report=term-missing
```

| Category | Tests |
|---|---|
| Schema validation | 4 tests — all tables and structure |
| Data quality | 8 tests — nulls, negatives, orphans, duplicates |
| KPI calculations | 6 tests — revenue, labour %, shrinkage, WoW |
| ETL logic | 5 tests — record counts, integrity, load accuracy |

---

## Skills Demonstrated

| Category | Skills |
|---|---|
| **SQL** | CTEs, Window Functions (LAG, RANK, STDDEV), Z-score anomaly detection, Bronze/Silver/Gold schema |
| **Python** | Pandas ETL, SQLAlchemy, synthetic data generation with seasonal patterns and UK bank holidays |
| **Statistics** | Z-score methodology, 12-week rolling baseline, per-store normalisation |
| **BI** | Power BI DAX (time intelligence, WoW variance, basket value), Row-Level Security, Deployment Pipeline |
| **Data Engineering** | Star schema — 3 fact tables + 3 dimensions, data quality validation layer |
| **Testing** | pytest fixtures, in-memory SQLite testing, CI/CD via GitHub Actions |

---

## Author

**Anandi Mahure** — Data Analyst | MSc Data Science, University of Bath (Dean's Award 2025)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://linkedin.com/in/anandirm)
[![GitHub](https://img.shields.io/badge/GitHub-Portfolio-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/anandi-mahure)

*Part of a production-grade data analytics portfolio targeting top DA/Analytics Engineer roles across UK, Dubai, and Australia.*

*Part of a production-grade data analytics portfolio targeting top DA/Analytics Engineer roles across UK, Dubai, and Australia.*
