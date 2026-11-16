# Project 1: The Modern Cloud Lakehouse & ELT Pipeline

## 📌 Overview
A scalable end-to-end data platform mimicking modern enterprise data warehousing workflows, transforming raw financial API data into a governed Medallion architecture (Bronze ➔ Silver ➔ Gold Star Schema).

---

## 🏗️ Architecture & Data Lineage Flow

1. **Upstream Source:** Yahoo Finance Public API (`yfinance`) generating daily OHLCV stock prices and company profiles[cite: 2.2].
2. **Bronze Layer (`data/bronze/`):** Immutable, Hive-partitioned Snappy Parquet files (`ingest_date=YYYY-MM-DD`) stamped with audit metadata (`_batch_id`, `_ingested_at`, `_source_system`)[cite: 2.2].
3. **Silver Layer (`dbt_project/models/silver/`):** Deduplicated, cleansed transactional tables (`silver_market_ohlcv`) and SCD Type 2 point-in-time snapshots (`snap_ticker_metadata`)[cite: 2.2].
4. **Gold Layer (`dbt_project/models/gold/`):** Business-ready Star Schema dimension and fact tables enriched with market cap tiering, 7-day rolling moving averages, and referential integrity constraints[cite: 2.2].

---

## 📂 Project Directory Tree

```text
modern-cloud-lakehouse/
├── dags/
│   └── market_lakehouse_dag.py        # Apache Airflow daily ELT orchestration DAG
├── data/
│   ├── bronze/                        # Hive-partitioned raw Parquet files (git-ignored)
│   └── warehouse/                     # Local DuckDB database file (lakehouse.duckdb)
├── dbt_project/
│   ├── models/
│   │   ├── gold/
│   │   │   ├── dim_assets.sql         # SCD Type 2 asset dimension + market cap tiers
│   │   │   ├── dim_date.sql           # Calendar dimension
│   │   │   └── fct_daily_market_trades.sql # Star Schema fact table with rolling averages
│   │   └── silver/
│   │       ├── sources.yml            # Source declarations & schema validation
│   │       └── silver_market_ohlcv.sql # Incremental deduplication & type casting
│   ├── snapshots/
│   │   └── snap_ticker_metadata.sql   # Slowly Changing Dimension Type 2 snapshot
│   ├── dbt_project.yml                # dbt configuration & materialization rules
│   └── profiles.yml                   # DuckDB / Snowflake connection profile
├── ingestion/
│   └── extract_market_data.py         # Yahoo Finance API extraction & audit stamper
├── transformations/
│   └── run_duckdb_warehouse.py        # Local DuckDB Medallion runner & DQ assertor
├── tests/
│   └── test_extract_market_data.py    # Pytest unit testing suite for Bronze schemas
├── requirements.txt                   # Python dependencies (duckdb, yfinance, pytest, etc.)
└── README.md
