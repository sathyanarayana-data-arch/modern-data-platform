"""Apache Airflow DAG for the Modern Cloud Lakehouse ELT Pipeline.

Orchestrates daily market data extraction into the Bronze Parquet layer,
followed by Silver/Gold Medallion transformations, SCD Type 2 tracking,
and automated data quality contract verification.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def alert_on_failure(context: dict) -> None:
    """Failure callback hook for Slack / PagerDuty / Email alerting."""
    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id if task_instance else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    execution_date = context.get("ds", "unknown_date")
    exception = context.get("exception")
    print(
        f"[ALERT] Pipeline failure | dag={dag_id} | task={task_id} "
        f"| execution_date={execution_date} | error={exception}"
    )


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="modern_cloud_lakehouse_daily_elt",
    description="End-to-end Bronze -> Silver -> Gold market data lakehouse pipeline",
    default_args=default_args,
    schedule="0 22 * * 1-5",  # Weekdays at 22:00 UTC after US market close
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "dbt", "duckdb", "elt"],
) as dag:

    # Task 1: Run pytest contract checks before ingesting
    run_unit_tests = BashOperator(
        task_id="run_unit_tests",
        bash_command="cd /opt/airflow/project && pytest -v",
    )

    # Task 2: Extract OHLCV + Ticker Metadata into Hive-partitioned Bronze Parquet
    ingest_bronze_market_data = BashOperator(
        task_id="ingest_bronze_market_data",
        bash_command="cd /opt/airflow/project && python ingestion/extract_market_data.py",
    )

    # Task 3: Execute Silver cleansing, SCD Type 2 snapshot, Gold Star Schema & DQ tests
    # Note: In Snowflake / Databricks production deployments, swap with:
    # `dbt snapshot && dbt run && dbt test` inside dbt_project/
    transform_silver_gold_warehouse = BashOperator(
        task_id="transform_silver_gold_warehouse",
        bash_command="cd /opt/airflow/project && python transformations/run_duckdb_warehouse.py",
    )

    # Task 4: Emit pipeline completion audit summary
    log_pipeline_completion = PythonOperator(
        task_id="log_pipeline_completion",
        python_callable=lambda **ctx: print(
            f"[SUCCESS] Lakehouse ELT completed for logical_date={ctx['ds']}"
        ),
    )

    run_unit_tests >> ingest_bronze_market_data >> transform_silver_gold_warehouse >> log_pipeline_completion
