import json
import oracledb
from airflow.hooks.base import BaseHook


def inserir_registro_teste(**context):
    conn_info = BaseHook.get_connection("oracle_adb_etl")
    extra = json.loads(conn_info.extra)

    connection = oracledb.connect(
        user=conn_info.login,
        password=conn_info.password,
        dsn=extra["dsn"],
    )
    cursor = connection.cursor()

    cursor.execute("SELECT NVL(MAX(nr_sequencia), 0) + 1 FROM ETL_USER.teste")
    proximo_nr = cursor.fetchone()[0]

    descricao = f"V2 AIRFLOW DAG - run_id={context['run_id']}"
    cursor.execute(
        "INSERT INTO ETL_USER.teste (nr_sequencia, descricao) VALUES (:1, :2)",
        [proximo_nr, descricao],
    )
    connection.commit()

    print(f"Inserido nr_sequencia={proximo_nr}, descricao='{descricao}'")

    cursor.close()
    connection.close()

    return proximo_nr
