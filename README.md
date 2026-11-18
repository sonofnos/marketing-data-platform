# marketing-data-platform

A production-grade marketing data platform built with **PySpark**, **PostgreSQL**, **Azure Data Factory**, and **Terraform for Azure**. Implements the medallion architecture (bronze → silver → gold) to ingest multi-source marketing data, run data quality checks, and produce RFM-based targeting segments for campaign delivery.

> PySpark is the runtime engine behind Azure Databricks — this project runs locally with the same API and deploys to Databricks/ADLS Gen2 via the included Terraform and ADF definitions.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Sources                                                            │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────┐  │
│  │CRM (CSV) │  │Campaigns(JSON│  │Events (JSONL)│  │Web Analyt.│  │
│  └────┬─────┘  └──────┬───────┘  └──────┬───────┘  └─────┬─────┘  │
└───────┼───────────────┼─────────────────┼────────────────┼─────────┘
        │  ADF CopyActivity / ADLS Gen2 raw zone            │
        ▼               ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Bronze  (raw parquet, partitioned by ingested_date)                │
│  crm_customers/  campaigns/  campaign_events/  web_analytics/       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  PySpark (Databricks notebook / job)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Silver  (cleaned, typed, PII-masked)                               │
│  customers/  campaign_events/  campaigns/                           │
│  • Dedup by key                                                     │
│  • Type casting + date parsing                                      │
│  • Email PII masked (****@domain.com)                               │
│  • Invalid / null rows dropped                                      │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  PySpark aggregations
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Gold  (business-ready aggregates)                                  │
│  campaign_metrics/  customer_ltv/  rfm_segments/                    │
│  • CTR, CVR, cost-per-conversion per campaign                       │
│  • 30/60/90-day LTV per customer                                    │
│  • RFM scores + segment labels (Champions → Hibernating)            │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Load to PostgreSQL serving layer
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PostgreSQL  (serving + reporting)                                  │
│  • 5 views: campaign_performance, customer_engagement,              │
│    high_value_customers, segment_summary, channel_attribution       │
│  • 3 stored procedures: sp_refresh_segments,                        │
│    sp_archive_old_events, sp_recompute_campaign_metrics             │
│  • 4 triggers: audit log, updated_at, guard active delete,          │
│    segment refresh flag                                             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Skills demonstrated

| Area | Implementation |
|------|----------------|
| **Azure Data Factory** | Real ADF pipeline JSON (`adf/`) with `CopyActivity`, `ExecuteDataFlow`, `IfCondition`, `StoredProcedure` activities, parameterised triggers |
| **Azure Databricks / Spark** | PySpark 3.5 (identical runtime to Databricks) — medallion transforms, window functions, Structured Streaming patterns |
| **Azure Data Lake (ADLS Gen2)** | Terraform provisions ADLS Gen2 with bronze/silver/gold filesystems, ADF linked service with managed identity |
| **Medallion architecture** | Bronze (raw) → Silver (cleaned) → Gold (aggregated) with clear layer contracts |
| **SQL depth** | 5 views with window functions & CTEs, 3 stored procedures with error handling, 4 triggers (audit, guard, computed columns) |
| **Data quality** | 4 check types: null-rate, dedup, referential integrity, value-range — all composable, `--strict` mode raises on failure |
| **Data integration** | Multi-source: CSV, JSON, JSONL, Parquet — each with its own ingest function and schema contract |
| **Campaign targeting** | RFM segmentation (Recency/Frequency/Monetary quintile scoring), `build_audience()` returns contact lists per segment |
| **IaC** | Terraform for Azure: resource group, ADLS Gen2, ADF, Databricks workspace, Azure SQL, Key Vault |
| **Testing** | 30+ pytest tests covering ingest, transform, segment, quality — PySpark in-process, no cluster needed |

---

## Quick start

```bash
# 1. Install dependencies
make install

# 2. Generate synthetic marketing data (2,000 customers, 20 campaigns, 50,000 events)
make data

# 3. Run the full pipeline (bronze → silver → gold → quality checks)
make pipeline

# 4. Run tests
make test

# 5. (Optional) Start PostgreSQL and load the serving layer
make db
psql -h localhost -U mktadmin -d marketing_platform < sql/01_schema.sql
psql -h localhost -U mktadmin -d marketing_platform < sql/02_views.sql
psql -h localhost -U mktadmin -d marketing_platform < sql/03_stored_procedures.sql
psql -h localhost -U mktadmin -d marketing_platform < sql/04_triggers.sql
```

---

## Deploying to Azure

```bash
cd terraform
terraform init
terraform plan -var="environment=prod"
terraform apply

# ADF pipelines: import via Azure Portal → Data Factory → Author → Import from JSON
# Or use: az datafactory pipeline create --factory-name <adf_name> --file @adf/pipeline_crm_ingest.json
```

The Databricks linked service (`adf/linked_services/azure_databricks.json`) connects ADF to the Databricks workspace provisioned by Terraform. PySpark jobs in `pipelines/` can be uploaded as Databricks notebooks or submitted as jobs with no code changes — the Spark session factory in `config.py` detects the environment.

---

## Project structure

```
marketing-data-platform/
├── data/
│   ├── generate_data.py          # Faker-based synthetic data generator
│   └── raw/                      # Generated seed files (CSV, JSON, Parquet)
├── pipelines/
│   ├── config.py                 # Paths + SparkSession factory
│   ├── ingest.py                 # Bronze: multi-source ingestion
│   ├── transform.py              # Silver + Gold: transforms & aggregations
│   ├── segment.py                # Gold: RFM segmentation + audience builder
│   ├── quality.py                # Data quality check framework
│   └── run_pipeline.py           # CLI entry point
├── sql/
│   ├── 01_schema.sql             # PostgreSQL tables + indexes
│   ├── 02_views.sql              # 5 reporting views
│   ├── 03_stored_procedures.sql  # 3 procedures
│   └── 04_triggers.sql           # 4 triggers
├── adf/
│   ├── pipeline_crm_ingest.json          # ADF pipeline: CRM → bronze → silver
│   ├── pipeline_campaign_events.json     # ADF pipeline: events → bronze → segments
│   └── linked_services/                  # ADLS Gen2, Databricks linked service JSON
├── terraform/
│   ├── main.tf                   # Azure: ADLS Gen2, ADF, Databricks, SQL, Key Vault
│   ├── variables.tf
│   └── outputs.tf
├── tests/
│   ├── conftest.py               # SparkSession fixture
│   ├── test_ingest.py
│   ├── test_transform.py
│   ├── test_segment.py
│   └── test_quality.py
├── docker-compose.yml            # PostgreSQL 16 + pgAdmin
└── Makefile
```
