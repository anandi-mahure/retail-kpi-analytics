# 🛒 Retail KPI Analytics System

[![CI Pipeline](https://github.com/anandi-mahure/retail-kpi-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/anandi-mahure/retail-kpi-analytics/actions)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-SQLite%2FPostgreSQL-4479A1?style=flat-square&logo=postgresql&logoColor=white)](#)
[![Power BI](https://img.shields.io/badge/Power_BI-DAX%20%7C%20RLS-F2C811?style=flat-square&logo=powerbi&logoColor=black)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)
[![pytest](https://img.shields.io/badge/Tests-pytest-blue?style=flat-square&logo=pytest&logoColor=white)](#)

End-to-end retail analytics system — SQL ETL pipeline, star schema data model, and Power BI KPI dashboard with DAX, RLS, and deployment pipeline. Processes **200,000+ transactional records**.

> **Business Impact:** Reduced reporting preparation time from 45 minutes to under 5 minutes. Enabled store managers to self-serve KPI data without analyst intervention.

---

## 📋 Table of Contents

- [Business Context](#-business-context)
- [Architecture](#-architecture)
- [Key Business Questions Answered](#-key-business-questions-answered)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Pipeline Overview](#-pipeline-overview)
- [SQL Modules](#-sql-modules)
- [Key Findings](#-key-findings)
- [Dashboard Preview](#-dashboard-preview)
- [Setup & Usage](#-setup--usage)
- [Test Suite](#-test-suite)
- [Skills Demonstrated](#-skills-demonstrated)

---

## 🏪 Business Context

Retail operations generate high volumes of transactional data across stores, products, and time periods. Without structured analytics infrastructure, reporting is manual, inconsistent, and slow — leaving managers without the insight to act.

This system builds a complete analytics layer on top of raw retail transactional data:

- **ETL pipeline** ingests and validates raw CSV data into a structured star schema
- **SQL KPI layer** calculates weekly sales performance, labour cost %, shrinkage anomalies, and week-on-week variance
- **Power BI dashboard** delivers self-serve reporting with row-level security by region

---

## 🏗 Architecture

```
Raw CSV Data
     │
     ▼
Python ETL Pipeline (generate_synthetic_data.py → load_to_database.py)
     │
     ▼
Star Schema (SQLite / PostgreSQL)
├── dim_store
├── dim_product
├── dim_date
└── fact_sales
     │
     ▼
SQL KPI Layer
├── 01_create_schema.sql          — Schema definition & DDL
├── 02_data_quality_validation.sql — Data quality checks
├── 03_kpi_labour_cost.sql        — Labour cost % & WoW variance
└── 04_shrinkage_anomaly_detection.sql — Z-score shrinkage detection
     │
     ▼
Power BI Dashboard
├── DAX measures (time intelligence, YTD, variance)
├── Row-Level Security (region-based)
└── Deployment pipeline configuration
```

<img src="https://raw.githubusercontent.com/anandi-mahure/retail-kpi-analytics/main/architecture/pipeline_diagram.png" width="800" alt="Pipeline Architecture"/>

---

## ❓ Key Business Questions Answered

| # | Business Question | SQL Module |
|---|---|---|
| 1 | What are weekly sales by store and category? | `03_kpi_labour_cost.sql` |
| 2 | Which stores have labour cost % above target? | `03_kpi_labour_cost.sql` |
| 3 | What is the week-on-week revenue variance? | `03_kpi_labour_cost.sql` |
| 4 | Which products show shrinkage anomalies? | `04_shrinkage_anomaly_detection.sql` |
| 5 | Are there data quality issues in the pipeline? | `02_data_quality_validation.sql` |
| 6 | What is revenue by region with RLS filtering? | Power BI DAX + RLS |

---

## 🛠 Tech Stack

| Layer | Technology |
|---|---|
| **Data Generation** | Python, Faker |
| **ETL Pipeline** | Python, Pandas, SQLAlchemy |
| **Database** | SQLite (dev) / PostgreSQL (prod-pattern) |
| **Data Modelling** | Star Schema — fact + 3 dimension tables |
| **SQL Analytics** | CTEs, Window Functions, Z-score anomaly detection |
| **BI & Reporting** | Power BI — DAX, Row-Level Security, Deployment Pipeline |
| **Testing** | pytest — 25 tests across schema, DQ, KPI, ETL |
| **CI/CD** | GitHub Actions |

---

## 📁 Project Structure

```
retail-kpi-analytics/
├── .github/
│   └── workflows/
│       └── ci.yml                    # CI pipeline — pytest on push
├── architecture/
│   └── pipeline_diagram.png          # System architecture diagram
├── data/                             # Raw CSV input data
├── docs/                             # Supporting documentation
├── powerbi/
│   └── screenshots/                  # Dashboard screenshots
├── python/
│   ├── generate_synthetic_data.py    # Synthetic retail data generator
│   └── load_to_database.py          # ETL loader — CSV to SQLite
├── sql/
│   ├── 01_create_schema.sql          # Star schema DDL
│   ├── 02_data_quality_validation.sql # DQ validation queries
│   ├── 03_kpi_labour_cost.sql        # KPI calculations
│   └── 04_shrinkage_anomaly_detection.sql # Anomaly detection
├── tests/
│   └── test_retail_pipeline.py       # 25-test pytest suite
├── .gitignore
├── CONTRIBUTING.md
├── LICENSE
├── README.md
└── requirements.txt
```

---

## ⚙️ Pipeline Overview

### Step 1 — Generate Synthetic Data
```bash
python python/generate_synthetic_data.py
```
Generates 200,000+ realistic retail transactions across multiple stores, products, and weeks.

### Step 2 — Load to Database
```bash
python python/load_to_database.py
```
Validates and loads CSV data into a star schema SQLite database.

### Step 3 — Run SQL Analytics
Execute SQL modules in order:
```sql
-- 1. Create schema
source sql/01_create_schema.sql

-- 2. Validate data quality
source sql/02_data_quality_validation.sql

-- 3. Calculate KPIs
source sql/03_kpi_labour_cost.sql

-- 4. Detect shrinkage anomalies
source sql/04_shrinkage_anomaly_detection.sql
```

---

## 🗄 SQL Modules

### `01_create_schema.sql`
Defines the star schema — `fact_sales` central table with `dim_store`, `dim_product`, and `dim_date` dimension tables. Includes indexes for query performance.

### `02_data_quality_validation.sql`
Automated data quality checks covering:
- Null value detection across all key fields
- Revenue calculation accuracy (quantity × unit_price tolerance)
- Orphaned foreign key detection
- Duplicate record identification

### `03_kpi_labour_cost.sql`
Core KPI layer using CTEs and window functions:
- Weekly sales by store and category
- Labour cost % of revenue vs target thresholds
- Week-on-week revenue variance using `LAG()`
- Month-to-date and year-to-date aggregations

### `04_shrinkage_anomaly_detection.sql`
Statistical anomaly detection using Z-score methodology:
- Calculates mean and standard deviation of shrinkage by category
- Flags products with Z-score > 2 as anomalous
- Enables proactive loss prevention intervention

---

## 📊 Key Findings

| Metric | Result |
|---|---|
| Records processed | 200,000+ transactions |
| Reporting time reduction | 45 minutes → under 5 minutes |
| Data quality checks | 8 automated validations |
| KPI calculations | Weekly, MTD, YTD, WoW variance |
| Shrinkage anomaly detection | Z-score threshold > 2.0 |
| Dashboard pages | Multi-page with RLS by region |

---

## 📈 Dashboard Preview

![Power BI Dashboard](powerbi/screenshots/)

*Power BI dashboard with DAX time intelligence, Z-score shrinkage detection, and row-level security by region.*

---

## 🚀 Setup & Usage

### Prerequisites
- Python 3.10+
- pip

### Installation

```bash
# Clone the repository
git clone https://github.com/anandi-mahure/retail-kpi-analytics.git
cd retail-kpi-analytics

# Install dependencies
pip install -r requirements.txt

# Generate synthetic data
python python/generate_synthetic_data.py

# Load to database
python python/load_to_database.py
```

---

## 🧪 Test Suite

25 tests across 4 categories:

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=python --cov-report=term-missing
```

| Test Category | Tests | Coverage |
|---|---|---|
| Schema validation | 4 | All tables and structure |
| Data quality | 8 | Nulls, negatives, orphans, duplicates |
| KPI calculations | 6 | Revenue, labour %, shrinkage, WoW |
| ETL logic | 5 | Record counts, integrity, loading |

---

## 🎯 Skills Demonstrated

| Category | Skills |
|---|---|
| **SQL** | CTEs, Window Functions (LAG, RANK), Z-score anomaly detection, Star Schema design |
| **Python** | Pandas ETL, SQLAlchemy, synthetic data generation, modular pipeline design |
| **BI** | Power BI DAX (time intelligence, YTD, variance), Row-Level Security, Deployment Pipeline |
| **Data Engineering** | Bronze-Silver-Gold architecture, data quality validation, star schema modelling |
| **Testing** | pytest fixtures, parameterised tests, CI/CD via GitHub Actions |

---

## 👩‍💻 Author

**Anandi Mahure** — Data Analyst | MSc Data Science, University of Bath (Dean's Award 2025)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://linkedin.com/in/anandirm)
[![GitHub](https://img.shields.io/badge/GitHub-Portfolio-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/anandi-mahure)

---

*Part of a production-grade data analytics portfolio targeting top DA/Analytics Engineer roles across UK, Dubai, and Australia.*
