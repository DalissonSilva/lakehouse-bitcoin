"""
Recria catalogo, schema e volume da camada Bronze caso tenham sido apagados
(ex: limpeza por inatividade no Databricks Free Edition). Idempotente -- todo
comando usa IF NOT EXISTS, entao pode ser rodado de novo sem efeito colateral
se os objetos ja existirem.
"""
import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()

DDLS = [
    ("Criando catalogo bitcoin", "CREATE CATALOG IF NOT EXISTS bitcoin"),
    ("Criando schema bitcoin.bronze", "CREATE SCHEMA IF NOT EXISTS bitcoin.bronze"),
    ("Criando volume bitcoin.bronze.raw", "CREATE VOLUME IF NOT EXISTS bitcoin.bronze.raw"),
]


def executar(w, warehouse_id, sql, descricao):
    print(f"\n--- {descricao} ---")
    resultado = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s"
    )
    print(f"Status: {resultado.status.state}")
    if resultado.status.state.value == "FAILED":
        erro = resultado.status.error
        print(f"ERRO: {erro}")
        raise RuntimeError(f"{descricao} falhou: {erro}")
    return resultado


def main():
    w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
    warehouse_id = list(w.warehouses.list())[0].id
    print(f"Usando warehouse: {list(w.warehouses.list())[0].name}")
    print("(primeira execução pode demorar ~30-60s para o warehouse ligar)")

    for descricao, sql in DDLS:
        executar(w, warehouse_id, sql, descricao)

    print("\nBronze pronta: bitcoin.bronze (+ volume raw). Agora rode incremental_binance.py -> databricks_upload.py -> databricks_merge_bronze.py.")


if __name__ == "__main__":
    main()
