# Modern Data Platform

End-to-end data engineering and cloud architecture implementations covering scalable lakehouse patterns, real-time Change Data Capture (CDC) streaming, Retrieval-Augmented Generation (RAG) vector pipelines, and automated DataOps CI/CD workflows.

## Architecture Modules

| Module | Focus Area | Core Capabilities |
| :--- | :--- | :--- |
| **[`modern-cloud-lakehouse`](./modern-cloud-lakehouse)** | Medallion Architecture & Cloud Warehousing | Multi-layered data modeling (Bronze/Silver/Gold), schema evolution, incremental loading, and query optimization. |
| **[`realtime-cdc-stream`](./realtime-cdc-stream)** | Event-Driven & Streaming Ingestion | Low-latency Change Data Capture pipelines, stream processing, and fault-tolerant event delivery. |
| **[`rag-vector-etl`](./rag-vector-etl)** | AI & Unstructured Data Pipelines | Document ingestion, text chunking, embedding generation, and vector database indexing for LLM retrieval. |
| **[`production-dataops-cicd`](./production-dataops-cicd)** | Automation, Testing & Governance | Automated CI/CD deployment workflows, data quality validation checks, and infrastructure-as-code practices. |

## Core Technology Stack

* **Languages & Frameworks:** Python, SQL, PySpark, Snowpark
* **Data Platforms & Storage:** Snowflake, Databricks, Delta Lake, Vector Databases
* **Orchestration & Streaming:** Apache Airflow, Apache Kafka, dbt
* **DevOps & Infrastructure:** GitHub Actions, Terraform, Docker

## Repository Structure

```text
modern-data-platform/
├── modern-cloud-lakehouse/   # Batch & incremental lakehouse pipelines
├── production-dataops-cicd/  # CI/CD workflows, testing, and deployment configs
├── rag-vector-etl/           # Embedding generation and vector store ETL
├── realtime-cdc-stream/      # Real-time streaming and CDC consumers/producers
└── README.md
