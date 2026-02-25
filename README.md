# Olist Data Engineering Pipeline

## Project Overview

This project builds an end-to-end data engineering pipeline using:

- Apache Airflow (orchestration)
- PostgreSQL (data warehouse)
- dbt (data transformation)
- Brazilian Olist E-commerce Dataset (Kaggle)

The pipeline ingests raw CSV data, transforms it into a star schema using dbt, and orchestrates the entire workflow with Airflow.

---

## Architecture (High-Level)

Raw CSV Files  
    ↓  
Airflow Ingestion DAG  
    ↓  
PostgreSQL (raw schema)  
    ↓  
dbt (staging → marts)  
    ↓  
Analytics Star Schema  

---

## Folder Structure

- `airflow/` → DAGs and ingestion scripts  
- `dbt_olist/` → dbt project  
- `data/` → raw data landing directory  
- `docker/` → container setup  
