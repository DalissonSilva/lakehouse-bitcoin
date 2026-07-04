from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys

sys.path.insert(0, '/opt/airflow/scripts')
from hello_git_sync import hello_git_sync

default_args = {
    "owner": "dalisson",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_hello_git_sync",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["teste", "git-sync", "github"],
    description="Valida integração GitHub → git-sync → Airflow"
) as dag:

    t1 = BashOperator(
        task_id="verifica_sync",
        bash_command="echo '=== git-sync info ===' && echo 'Pasta dags:' && ls /opt/airflow/dags/ && echo 'Horário:' $(date)",
    )

    t2 = PythonOperator(
        task_id="hello_git_sync",
        python_callable=hello_git_sync,
    )

    t1 >> t2
