# Fintech Data Pipeline

> Production-grade credit card data pipeline — Fiserv API → AWS → Snowflake → dbt → Dashboards

Built from patterns used in production fintech to process **100M+ daily transactions** for 1M+ active cardholders with **99.9% SLA** and **$140K annual cost savings**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     FINTECH DATA PIPELINE                            │
│                                                                       │
│  Fiserv API          AWS                    Snowflake                 │
│  (Payment           ┌──────────┐           ┌────────────────────┐    │
│   Processor)        │  S3 Raw  │  COPY INTO │  RAW.TRANSACTIONS  │    │
│      │              │  Bucket  │──────────►│  RAW.PAYMENTS      │    │
│      │  REST API    │          │           └──────────┬─────────┘    │
│      ├─────────────►│ JSON     │                      │              │
│      │  (OAuth2)    │ partitioned           dbt (incremental)        │
│      │              │ by date   │           ┌──────────▼─────────┐    │
│      │              └────┬─────┘           │  STAGING views     │    │
│      │                   │                 │  INTERMEDIATE mvs  │    │
│      │           EMR     │                 │  MARTS facts       │    │
│      │         Serverless│  Parquet        └──────────┬─────────┘    │
│      │           (Spark)─┘                            │              │
│      │                                     ┌──────────▼─────────┐    │
│      │                                     │  Executive Dashboards│   │
│      │                                     │  Regulatory Reports  │   │
│      │                                     │  Credit Analytics    │   │
│      │                                     └────────────────────┘    │
│      │                                                                │
│  Airflow orchestrates all steps (hourly, 4h SLA)                     │
│  Terraform manages all AWS + Snowflake infrastructure                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Results

| Metric | Before | After |
|---|---|---|
| Batch processing time | 24 hours | **4 hours** (83% reduction) |
| Snowflake query p95 latency | 5s | **2s** (60% improvement) |
| Annual infrastructure cost | $400K | **$260K** (35% / $140K saved) |
| Pipeline uptime | 97% | **99.9% SLA** |
| Cardholders supported | 10K (pilot) | **1M+** |

---

## Stack

| Layer | Technology |
|---|---|
| **Ingestion** | Python, Fiserv REST API, OAuth2, boto3 |
| **Processing** | PySpark on AWS EMR Serverless |
| **Storage** | AWS S3 (raw + processed), Snowflake |
| **Transformation** | dbt (incremental merge, custom macros) |
| **Orchestration** | Apache Airflow (hourly, dependency-managed) |
| **Infrastructure** | Terraform (AWS + Snowflake provider) |
| **Compliance** | CFPB, FDIC, PCI-DSS patterns |

---

## Project Structure

```
fintech-data-pipeline/
├── terraform/
│   ├── main.tf             # AWS + Snowflake infrastructure
│   ├── variables.tf
│   └── outputs.tf
├── dbt/
│   ├── dbt_project.yml
│   ├── macros/
│   │   ├── json_parse.sql          # Fiserv JSON parsing macro
│   │   └── incremental_merge.sql   # Incremental predicate helpers
│   └── models/
│       ├── staging/
│       │   ├── stg_transactions.sql
│       │   └── stg_payments.sql
│       ├── intermediate/
│       │   └── int_credit_card_events.sql
│       └── marts/
│           └── fct_credit_card_daily.sql
├── pipelines/
│   ├── ingest/
│   │   ├── fiserv_api_consumer.py  # Paginated API extraction
│   │   └── s3_to_snowflake.py      # COPY INTO orchestration
│   └── spark/
│       └── transaction_processor.py # EMR Serverless PySpark job
├── dags/
│   └── fintech_pipeline_dag.py     # Airflow DAG (hourly)
└── requirements.txt
```

---

## Setup

### 1. Infrastructure

```bash
cd terraform
terraform init
terraform plan -var-file=prod.tfvars
terraform apply
```

### 2. Python dependencies

```bash
pip install -r requirements.txt
cp .env.example .env
# Fill in Snowflake, AWS, and Fiserv credentials
```

### 3. dbt

```bash
cd dbt
dbt deps
dbt debug          # Verify Snowflake connection
dbt run            # Run all models
dbt test           # Validate output
```

### 4. Airflow

```bash
cp dags/fintech_pipeline_dag.py $AIRFLOW_HOME/dags/
airflow dags trigger fintech_credit_card_pipeline
```

---

## dbt Models

### Staging layer (`stg_*`)
- Views over raw Snowflake tables
- JSON parsing via custom `parse_fiserv_transaction()` macro
- Deduplication using `ROW_NUMBER()` window functions

### Intermediate layer (`int_*`)
- Incremental merge with 6h lookback for late-arriving payments
- Joins transactions + payments
- Adds derived fields (geo_type, amount_tier, is_approved)

### Marts layer (`fct_*`)
- Daily aggregated fact table for dashboards
- Clustered by `event_date` for 60% query speedup
- Powers executive dashboards, CFPB/FDIC regulatory reporting

---

## Snowflake Cost Optimization

- **Clustering keys** on `event_date` — eliminates full table scans
- **Materialized views** for common aggregation patterns
- **Result caching** — repeated dashboard queries cost $0
- **Zero-copy clones** for dev/staging environments
- **Auto-suspend** warehouse after 60s idle

---

## About

Built by **Sneha Bankapalli** — Senior Data Engineer at a fintech company.
These patterns power real production pipelines processing 100M+ daily events.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-sneha2095-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/sneha2095/)
[![GitHub](https://img.shields.io/badge/GitHub-Snehabankapalli-181717?style=flat&logo=github)](https://github.com/Snehabankapalli)
