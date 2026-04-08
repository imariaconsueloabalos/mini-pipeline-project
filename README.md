# 🛒 Olist E-Commerce Analytics Pipeline

> A modern, end-to-end data engineering pipeline that transforms raw Olist e-commerce CSV data into an analytics-ready Star Schema data warehouse — built with Python, dbt, Apache Airflow, and Docker.

---

## Project Overview

This project implements a production-style ETL pipeline built as a data engineering internship mini-project. It ingests raw transactional CSV data from the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), applies business logic transformations using **dbt**, stores all results in a **PostgreSQL** warehouse under a `processed` schema, and is fully orchestrated via **Apache Airflow** — all running inside a **Docker** environment.

### Business Goals
- Revenue and payment analysis
- Seller performance tracking
- Customer segmentation
- Time-based sales trends

---

## Architecture (High-Level)

### High-Level Pipeline Flow
```
olist_*.csv files  (data/raw/)
        │
        ▼
┌────────────────────────┐
│    ingest_olist.py     │  BashOperator
│  strips "olist_" prefix│  loads CSVs → raw schema via pandas + SQLAlchemy
└────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│                  PostgreSQL — raw schema                   │
│  customers_dataset       order_payments_dataset            │
│  geolocation_dataset     order_reviews_dataset             │
│  order_items_dataset     products_dataset                  │
│  orders_dataset          sellers_dataset                   │
│  product_category_name_translation                         │
└────────────────────────────────────────────────────────────┘
        │
        ▼  dbt seed
┌────────────────────────┐
│   product_category_    │  Loads translation CSV lookup into processed schema
│   name_translation     │
└────────────────────────┘
        │
        ▼  dbt run --full-refresh
┌────────────────────────────────────────────────────────────┐
│               PostgreSQL — processed schema                │
│                                                            │
│  VIEWS (Staging)          TABLES (Marts)                   │
│  stg_customers            dim_customers                    │
│  stg_geolocation          dim_sellers                      │
│  stg_orders               dim_products                     │
│  stg_order_items          dim_date                         │
│  stg_order_payments       fact_sales                       │
│  stg_order_reviews        fact_reviews                     │
│  stg_products                                              │
│  stg_sellers                                               │
│  stg_product_category_name_translation                     │
└────────────────────────────────────────────────────────────┘
        │
        ▼  dbt test
┌────────────────────────┐
│   Data Quality Tests   │  unique, not_null, relationships
└────────────────────────┘
```

### Airflow DAG: `olist_final_pipeline`
Schedule: `@daily` | Start date: `2026-02-25` | Catchup: `False`

```
run_ingestion
    │  BashOperator → python ingest_olist.py
    ▼
dbt_seed
    │  BashOperator → dbt seed
    ▼
dbt_run_all_models
    │  BashOperator → dbt run --full-refresh
    ▼
dbt_test
      BashOperator → dbt test
```

---

## Folder Structure

- `airflow/` → DAGs and ingestion scripts  
- `dbt_olist/` → dbt project  
- `data/` → raw data landing directory  
- `docker/` → container setup  

---

## 🔩 Key dbt Models

### Staging Layer (`models/staging/`) — Views

Light-touch cleaning only. No business logic. Each model maps 1:1 to a raw table.

| Model | Raw Source Table | Key Transformations |
|---|---|---|
| `stg_customers` | `customers_dataset` | `lpad()` zip codes, `initcap(trim())` city, `upper()` state |
| `stg_geolocation` | `geolocation_dataset` | Cast zip prefix, standardize lat/lng |
| `stg_orders` | `orders_dataset` | Cast 6 timestamp columns from `text` → `timestamp` |
| `stg_order_items` | `order_items_dataset` | Cast price/freight to `numeric`, rename `shipping_limit_ts` |
| `stg_order_payments` | `order_payments_dataset` | Normalize `payment_type`, cast `payment_value` |
| `stg_order_reviews` | `order_reviews_dataset` | Cast `review_creation_date` and `review_answer_timestamp` |
| `stg_products` | `products_dataset` | Fix raw typos (`lenght` → `length`), cast dimensions to `numeric` |
| `stg_sellers` | `sellers_dataset` | `lpad()` zip codes, clean city/state |
| `stg_product_category_name_translation` | `product_category_name_translation` | Pass-through from seed |

### Marts Layer (`models/marts/`) — Tables

#### Dimension Models

| Model | Key Logic |
|---|---|
| `dim_customers` | Aggregates geolocation by zip (avg lat/lng, 2 decimal places), generates `customer_key` |
| `dim_sellers` | Same geolocation enrichment pattern as customers, generates `seller_key` |
| `dim_products` | Joins English category names from seed, generates `product_key` |
| `dim_date` | Expands purchase timestamps into year, month, day, quarter, weekday attributes |

#### Fact Models

| Model | Key Logic |
|---|---|
| `fact_sales` | Aggregates payments per order, joins all 4 dims, allocates `payment_allocated` proportionally via window function |
| `fact_reviews` | Joins reviews to orders and order items to resolve `customer_key` and `seller_key` |

### Payment Allocation Logic (`fact_sales`)
```sql
price / sum(price) over (partition by order_id) * payment_value
```
Each order item receives a share of the total payment proportional to its price — handling multi-item orders correctly.

---

## 🚀 How to Run

### Prerequisites
- Docker & Docker Compose installed
- Olist CSV files placed in `data/raw/` (named `olist_*.csv`)
- `.env` file configured with your PostgreSQL credentials

### 1. Start all services
```bash
docker-compose up -d
```

### 2. Access Airflow UI
```
http://localhost:8080
```
Trigger the `olist_final_pipeline` DAG manually or let it run on its `@daily` schedule.

### 3. Monitor task execution
In the Airflow UI, click into the DAG and verify all 4 tasks turn green:
```
run_ingestion → dbt_seed → dbt_run_all_models → dbt_test
```

---

## Challenges Overcome

1. **Windows Volume Compatibility** — dbt logs and compiled files redirected to `/tmp/dbt/` to bypass Windows Docker volume write restrictions using `--log-path` and `--target-path` flags
2. **Schema Alignment** — Resolved column name mismatches between staging (`*_id`) and mart layers (`*_key`) causing relationship test failures
3. **Data Quality Anomaly** — Identified 3 delivered orders with no payment records; retained with `NULL` and documented rather than dropped
4. **Fact Table Grain** — Ensured payment values were correctly aggregated at order level before proportional allocation at order-item grain
5. **YAML & Test Config Errors** — Fixed dbt `schema.yml` parsing issues and updated deprecated test syntax
6. **Surrogate Key Design** — Implemented formatted surrogate keys (`'C' || lpad(row_number()...)`) instead of hash-based keys for readability

---

Maria Consuelo Abalos

