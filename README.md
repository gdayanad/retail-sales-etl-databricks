markdown# Retail Sales ETL Pipeline
### A Portfolio Data Engineering Project on Databricks

---

## Project Summary
An end-to-end data engineering pipeline that processes retail sales data 
from raw CSV and JSON files into a clean star schema data mart, 
ready for BI dashboards.

Built on Databricks Community Edition using PySpark, Delta Lake, 
Unity Catalog, and Databricks Workflows.

---

## Architecture
Raw Files (CSV/JSON)
↓
Bronze Layer  →  raw data preserved as Delta tables
↓
Silver Layer  →  cleaned, validated, quarantine pattern
↓
Gold Layer    →  star schema + KPI view for dashboards
↓
Data Quality  →  10 automated tests
↓
Orchestration →  daily Databricks Workflow at 8am

---

## Tech Stack
- **Platform:** Databricks Community Edition
- **Language:** PySpark + SQL
- **Storage:** Delta Lake, Unity Catalog Volumes
- **Orchestration:** Databricks Workflows
- **Architecture:** Medallion (Bronze / Silver / Gold)

---

## Data Sources

| Source | Format | Rows | Description |
|--------|--------|------|-------------|
| orders | CSV | 9 raw / 6 clean | Daily order transactions |
| customers | JSON | 5 | Customer master data |
| products | CSV | 5 | Product catalog |

---

## Medallion Architecture

### Bronze Layer — Raw Data
| Table | Rows | Description |
|-------|------|-------------|
| bronze_orders | 9 | Raw orders including bad rows |
| bronze_customers | 5 | Raw customer JSON |
| bronze_products | 5 | Raw product CSV |

### Silver Layer — Clean Data
| Table | Rows | Description |
|-------|------|-------------|
| silver_orders | 6 | Cleaned, deduplicated orders |
| silver_customers | 5 | Standardized customers |
| silver_products | 5 | Typed product data |
| quarantine_orders | 2 | Bad rows with reason labels |

### Gold Layer — Star Schema
| Table | Rows | Description |
|-------|------|-------------|
| fact_orders | 6 | Central fact table, grain = one order |
| dim_customer | 5 | Customer dimension |
| dim_product | 5 | Product dimension |
| dim_date | 1096 | Pre-generated date dimension |
| dim_channel | 3 | Sales channel dimension |
| vw_sales_kpis | view | KPI view for dashboards |

---

## Data Quality Tests
10 automated tests run after every pipeline execution:

1. Row count check — silver_orders has exactly 6 rows
2. Null check — no nulls in critical columns
3. Uniqueness — order_id is unique
4. No negative amounts — all amounts > 0
5. Referential integrity — every order has valid customer
6. Referential integrity — every order has valid product
7. Revenue reconciliation — bronze total matches silver total
8. Freshness check — latest order date is correct
9. Gross profit sanity — no negative profit margins
10. Schema check — all expected columns exist

---

## Intentional Bad Data (for testing)

| Row | Issue | Handling |
|-----|-------|----------|
| ORD006 | Missing customer_id | Quarantined |
| ORD007 | Negative amount (-149.99) | Quarantined |
| ORD001 | Duplicate row | Deduplicated |

---

## Key Engineering Patterns
- **Medallion Architecture** — Bronze / Silver / Gold layers
- **Quarantine pattern** — bad rows never silently deleted
- **Idempotent writes** — safe to rerun without duplicates
- **Surrogate keys** — integer keys for fast dimension joins
- **Partitioning** — fact_orders partitioned by order_date
- **Delta Lake** — ACID transactions and time travel
- **Lazy evaluation** — PySpark optimises before executing

---

## Project Structure
Notebooks:
01_setup_and_landing     create folders, write sample data
02_bronze_ingestion      raw files to Bronze Delta tables
03_silver_transform      Bronze to Silver, clean + quarantine
04_gold_star_schema      Silver to Gold, star schema + KPIs
05_data_quality          10 automated quality tests
06_orchestration_check   final pipeline health check

---

## How to Run
1. Open Databricks workspace
2. Run notebooks 01 through 06 in order
3. OR trigger Databricks Workflow: `retail_sales_etl_pipeline`
4. Check run status under Jobs and Pipelines → Runs

---

## KPIs Available
- Total revenue by month, quarter, year
- Order count by channel (web, store, app)
- Average order value by customer segment
- Gross profit and margin % by product category
- Weekday vs weekend sales comparison
- Revenue by region and country

---

## Known Limitations and Next Steps
- Sample dataset is small — production would use incremental loads
- Row counts are hardcoded — production would use dynamic checks
- No SCD Type 2 implemented for slowly changing dimensions
- Would add dbt for transformation layer in production

---

## Author
Built as a portfolio project — Medallion Architecture on Databricks.
