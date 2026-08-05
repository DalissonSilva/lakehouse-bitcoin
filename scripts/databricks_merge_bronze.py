"""
Primeiro MERGE INTO -- cria e popula a tabela Delta bronze.btc_ohlcv
a partir dos arquivos Parquet no Volume.
"""
import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()

DDL_CRIACAO = """
CREATE TABLE IF NOT EXISTS bitcoin.bronze.btc_ohlcv (
    open_time        TIMESTAMP,
    close_time       TIMESTAMP,
    open             DOUBLE,
    high             DOUBLE,
    low              DOUBLE,
    close            DOUBLE,
    volume           DOUBLE,
    num_trades       BIGINT,
    fechada          BOOLEAN,
    ingest_ts_utc    STRING,
    source_symbol    STRING
) USING DELTA
"""

MERGE = """
MERGE INTO bitcoin.bronze.btc_ohlcv AS target
USING (
    SELECT open_time, close_time, open, high, low, close, volume,
           num_trades, fechada, ingest_ts_utc, source_symbol
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY open_time
                   ORDER BY fechada DESC, ingest_ts_utc DESC
               ) AS rn
        FROM parquet.`/Volumes/bitcoin/bronze/raw/btc/`
    )
    WHERE rn = 1
) AS source
ON target.open_time = source.open_time
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


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

    executar(w, warehouse_id, DDL_CRIACAO, "Criando tabela Delta (se não existir)")
    executar(w, warehouse_id, MERGE, "Executando MERGE INTO")

    print("\n--- Verificando resultado ---")
    resultado = executar(
        w, warehouse_id,
        """SELECT COUNT(*) as total, MIN(open_time) as inicio, MAX(open_time) as fim,
                  SUM(CASE WHEN fechada THEN 0 ELSE 1 END) as em_aberto
           FROM bitcoin.bronze.btc_ohlcv""",
        "Contando linhas na Bronze"
    )
    if resultado.result and resultado.result.data_array:
        total, inicio, fim, em_aberto = resultado.result.data_array[0]
        print(f"Total de linhas:      {total}")
        print(f"Período:              {inicio} até {fim}")
        print(f"Velas ainda abertas:  {em_aberto}")


if __name__ == "__main__":
    main()
