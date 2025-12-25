# Ad Campaign Analytics Pipeline

An end-to-end data engineering project that automates the extraction, transformation, and analysis of advertising campaign data using modern data stack tools.

## Project Overview

This project demonstrates a complete data pipeline that ingests advertising campaign data from Kaggle, loads it into Google BigQuery, and transforms it using dbt for analytics-ready insights.

## Tech Stack & Tools

- **Apache Airflow** - Workflow orchestration and scheduling
- **Google BigQuery** - Cloud data warehouse for storage and querying
- **dbt (Data Build Tool)** - Data transformation and modeling
- **Docker** - Containerization for reproducible deployments
- **Python** - ETL scripting and data processing
- **Kaggle API** - Data source integration

## Key Features

### Data Engineering
- **Automated Data Ingestion**: Weekly scheduled pipeline that extracts Facebook ad campaign data from Kaggle using the Kaggle API
- **Cloud Data Warehousing**: Loads raw data into BigQuery with automatic schema detection and incremental loading
- **Containerized Environment**: Fully Dockerized setup with custom Airflow image using `uv` for fast dependency management
- **Infrastructure as Code**: Docker Compose configuration for easy deployment and environment replication

### Orchestration
- **Apache Airflow DAG**: Orchestrates the entire ETL pipeline with proper task dependencies and error handling
- **Scheduled Execution**: Automated weekly runs every Monday at 6 AM UTC
- **Retry Logic**: Built-in fault tolerance with configurable retry attempts and delays
- **Monitoring & Logging**: Complete task execution tracking through Airflow UI

### Data Pipeline Architecture
- **Raw Data Layer**: Initial data landing zone in BigQuery (`ad_campaign_raw` dataset)
- **dbt Transformations**: (In progress) Data modeling layer for creating analytics-ready tables
- **Authentication Management**: Secure credential handling for Google Cloud and Kaggle APIs using environment variables
- **Modular Design**: Separated concerns with dedicated folders for DAGs, BigQuery scripts, and dbt models

## Data layers & presentation 

We use a layered data presentation to separate responsibilities and make testing, lineage, and access control straightforward.

- **Raw / Landing (`ad_campaign_raw`)**
	- Raw source payloads as ingested from Kaggle. Minimal changes (autodetect schema, append) and original metadata preserved for replayability.

- **Staging (`stg_` models / `ad_campaign_staging`)**
	- Lightweight cleaning and normalization (types, timestamps, deduplication, canonical column names). Staging models are the single source for downstream transformations.
	- Typical dbt materialization: table or incremental table. Standard tests: `not_null`, `unique`, `accepted_values`.

- **Intermediate (`int_` models / `ad_campaign_intermediate`)**
	- Enrichment and denormalization for complex logic and intermediate aggregations. Materialized as persisted (often incremental) tables partitioned by date for performance.

- **Mart (`mart_` models / `ad_campaign_mart`)**
	- Analytics-ready tables (facts and dimensions) optimized for reporting and dashboards. Strong schema contracts, partitioning, clustering and access controls applied.
	- Changes to marts require CI (dbt tests) and a documented migration plan when breaking schema changes are introduced.

Conventions:

- Prefix dbt models with `stg_`, `int_`, or `mart_` and store mart models in `ad_campaign_mart` dataset.
- Prefer date partitioning and clustering for mart tables to optimize cost and performance.
- Use dbt tests and snapshots for SCDs; include owner and short description in model docs.

This layered presentation keeps the pipeline modular, testable, and easy to promote through `model` → `intermediate` → `staging` → `main`.

## Technical Implementation

### Pipeline Components
1. **Dataset Creation**: Automatically provisions BigQuery datasets if they don't exist
2. **Data Loading**: Python-based ETL script using `kagglehub` and Google Cloud BigQuery client libraries
3. **Error Handling**: Graceful failure handling with detailed logging for troubleshooting
4. **Environment Configuration**: Centralized configuration through environment variables and dbt profiles

### Development Practices
- **Version Control**: Git-tracked project with proper `.gitignore` for sensitive credentials
- **Volume Mounting**: Live code reloading during development without rebuilding containers
- **Python Path Management**: Custom PYTHONPATH configuration for seamless module imports
- **Dependency Management**: Modern Python packaging with `pyproject.toml` and `uv` compiler

## Project Structure

```
├── dags/                      # Airflow DAG definitions
│   └── ad_campaign_pipeline.py
├── bigquery/                  # BigQuery ETL scripts
│   └── load_kaggle_data.py
├── dbt/                       # dbt project (transformations coming soon)
│   ├── models/
│   ├── profiles.yml
│   └── dbt_project.yml
├── keys/                      # Credentials (gitignored)
│   ├── gcp-service-account.json
│   └── kaggle.json
├── docker-compose.yml         # Service orchestration
├── Dockerfile                 # Custom Airflow image
└── pyproject.toml            # Python dependencies
```

## 🔧 Configuration Highlights

- **Airflow**: Sequential executor with SQLite backend for lightweight local development
- **BigQuery Location**: EU region with configurable location settings
- **Authentication**: Service account-based authentication for GCP with automatic connection setup
- **DAG Settings**: Disabled example DAGs, auto-unpaused new DAGs for immediate testing

## Skills Demonstrated

- Cloud data warehousing (Google BigQuery)
- Workflow orchestration (Apache Airflow)
- Containerization and DevOps (Docker)
- Python programming and scripting
- ETL/ELT pipeline design
- API integration (Kaggle, Google Cloud)
- Infrastructure as Code
- Modern data stack tooling


