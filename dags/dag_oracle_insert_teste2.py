from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow/scripts_root/current/scripts')
from oracle_insert_teste2 import inserir_registro_teste

default_args = {
    "owner": "dalisson",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_oracle_insert_teste2",
    schedule="*/3 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["oracle", "adb", "teste", "git-sync"],
    description="Teste 2 que insere registro sequencial na tabela ETL_USER.teste a cada 5 minutos",
) as dag:

    t1 = PythonOperator(
        task_id="inserir_registro",
        python_callable=inserir_registro_teste,
    )
