"""
Sobe os Parquets locais para o Volume bitcoin.bronze.raw no Databricks.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()


def main():
    w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])

    base_local = Path("/opt/airflow/landing_simulado/btc")
    arquivos = sorted(base_local.rglob("*.parquet"))

    if not arquivos:
        print("Nenhum arquivo Parquet encontrado.")
        return

    for arquivo in arquivos:
        particao = arquivo.parent.name  # ex: ingest_date=2026-07-05
        caminho_remoto = f"/Volumes/bitcoin/bronze/raw/btc/{particao}/{arquivo.name}"

        print(f"Enviando: {arquivo}")
        print(f"      -> {caminho_remoto}")

        with open(arquivo, "rb") as f:
            w.files.upload(caminho_remoto, f, overwrite=True)

        print("  OK\n")

    print("--- Conteúdo do Volume após upload ---")
    for item in w.files.list_directory_contents(f"/Volumes/bitcoin/bronze/raw/btc/{particao}/"):
        print(f"  {item.path}  ({item.file_size} bytes)")


if __name__ == "__main__":
    main()
