import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/scripts_root/current/scripts")

from databricks_bootstrap_schema import main as bootstrap_schema
from incremental_binance import main as extrair_incremental
from databricks_upload import main as upload_parquet
from databricks_merge_bronze import main as merge_bronze

default_args = {
    "owner": "dalisson",
    "retries": 1,
}

with DAG(
    dag_id="btc_bronze_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["bitcoin", "bronze", "databricks"],
) as dag:

    task_bootstrap = PythonOperator(
        task_id="bootstrap_schema_bronze",
        python_callable=bootstrap_schema,
    )

    task_extrair = PythonOperator(
        task_id="extrair_incremental_binance",
        python_callable=extrair_incremental,
    )

    task_upload = PythonOperator(
        task_id="upload_parquet_databricks",
        python_callable=upload_parquet,
    )

    task_merge = PythonOperator(
        task_id="merge_into_bronze",
        python_callable=merge_bronze,
    )

    task_bootstrap >> task_extrair >> task_upload >> task_merge