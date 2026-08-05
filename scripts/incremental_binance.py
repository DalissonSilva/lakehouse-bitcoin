"""
Carga incremental Binance -> Parquet (landing), com duas trilhas independentes:

1. CONFIRMADO  -- watermark avança só sobre velas já FECHADAS (histórico definitivo)
2. VELA ABERTA -- sempre re-busca e sobrescreve a vela do período corrente,
                  até ela fechar; nunca avança o watermark confirmado.

O MERGE INTO na Bronze trata as duas trilhas de forma unificada: pela chave
open_time, tanto faz se a linha veio do arquivo histórico ou do arquivo da
vela aberta -- update se já existe, insert se não existe.

O watermark da trilha CONFIRMADO não é mais um arquivo local: é lido direto
via MAX(close_time) na própria tabela Bronze, que é a fonte da verdade sobre
o que já foi de fato persistido (evita watermark local divergir do estado
real quando um MERGE falha, e elimina estado em disco entre execuções).
"""
import os
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()

URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1d"
LANCAMENTO_PAR = datetime(2017, 8, 17, tzinfo=timezone.utc)

CAMPOS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "num_trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]

LANDING_BASE = Path("/opt/airflow/landing_simulado/btc")

DURACAO_INTERVALO = {
    "1m": timedelta(minutes=1), "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15), "1h": timedelta(hours=1),
    "4h": timedelta(hours=4), "1d": timedelta(days=1),
}


def parse_klines(raw: list) -> pd.DataFrame:
    """Converte a lista bruta da API em DataFrame tipado -- usado pelas duas trilhas."""
    if not raw:
        return pd.DataFrame(columns=CAMPOS)
    df = pd.DataFrame(raw, columns=CAMPOS)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df["num_trades"] = df["num_trades"].astype("int64")
    return df.drop(columns=["ignore", "quote_asset_volume", "taker_buy_base", "taker_buy_quote"])


def chamar_api(params: dict) -> list:
    resp = requests.get(URL, params=params, timeout=10)
    peso = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", 0))
    if peso > 5000:
        print(f"  peso alto ({peso}/6000) -- pausando 10s")
        time.sleep(10)
    if resp.status_code == 429:
        espera = int(resp.headers.get("Retry-After", 5))
        print(f"  429 recebido -- aguardando {espera}s")
        time.sleep(espera)
        return chamar_api(params)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# TRILHA 1 -- CONFIRMADO (só vela fechada, watermark avança)
# ---------------------------------------------------------------------------

def ler_watermark() -> datetime | None:
    """MAX(close_time) confirmado direto na Bronze -- None se a tabela ainda não existe/está vazia."""
    w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
    warehouse_id = list(w.warehouses.list())[0].id
    resultado = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="SELECT MAX(close_time) FROM bitcoin.bronze.btc_ohlcv WHERE fechada = true",
        wait_timeout="30s",
    )
    if resultado.status.state.value == "FAILED":
        print(f"  aviso: nao foi possivel ler watermark na Bronze ({resultado.status.error}) -- assumindo bootstrap")
        return None
    linha = resultado.result.data_array[0] if resultado.result and resultado.result.data_array else [None]
    valor = linha[0]
    return datetime.fromisoformat(valor) if valor else None


def corte_seguro(interval: str) -> datetime:
    """Início do período corrente -- tudo ANTES disso já fechou."""
    agora = datetime.now(timezone.utc)
    duracao = DURACAO_INTERVALO[interval]
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    return agora - (agora - epoch) % duracao


def buscar_intervalo_completo(inicio: datetime, fim: datetime) -> pd.DataFrame:
    """Pagina o intervalo [inicio, fim). CORRIGIDO: endTime exclusivo (-1ms),
    nunca deixa a vela em formação vazar para dentro do histórico confirmado."""
    todas = []
    cursor = inicio
    while cursor < fim:
        params = {
            "symbol": SYMBOL, "interval": INTERVAL,
            "startTime": int(cursor.timestamp() * 1000),
            "endTime": int(fim.timestamp() * 1000) - 1,   # <-- CORREÇÃO do bug
            "limit": 1000,
        }
        lote = chamar_api(params)
        if not lote:
            break
        todas.extend(lote)
        ultimo_close_ms = lote[-1][6]
        cursor = datetime.fromtimestamp(ultimo_close_ms / 1000, tz=timezone.utc) + timedelta(milliseconds=1)
        if len(lote) < 1000:
            break
        time.sleep(0.3)
    return parse_klines(todas)


def processar_confirmado():
    print("=" * 70)
    print("TRILHA 1 -- CONFIRMADO (velas fechadas)")
    print("=" * 70)

    watermark = ler_watermark()
    fim = corte_seguro(INTERVAL)
    inicio = watermark if watermark else LANCAMENTO_PAR

    if watermark is None:
        print(f"Nenhum dado confirmado na Bronze -- BOOTSTRAP desde {LANCAMENTO_PAR.date()}")
    else:
        print(f"Watermark (MAX close_time confirmado na Bronze): {watermark.isoformat()}")

    if inicio >= fim:
        print("Nada novo para buscar -- ultima vela fechada ja processada.")
        return

    print(f"Buscando de {inicio.isoformat()} ate {fim.isoformat()} (exclusive)")
    df = buscar_intervalo_completo(inicio, fim)

    if df.empty:
        print("Retorno vazio.")
        return

    ingest_date = datetime.now(timezone.utc).date().isoformat()
    pasta = LANDING_BASE / f"ingest_date={ingest_date}"
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"btc_ohlcv_{datetime.now(timezone.utc).strftime('%H%M%S')}.parquet"
    df["ingest_ts_utc"] = datetime.now(timezone.utc).isoformat()
    df["source_symbol"] = SYMBOL
    df["fechada"] = True
    df.to_parquet(arquivo, engine="pyarrow", index=False, compression="snappy")

    print(f"{len(df)} linhas gravadas em {arquivo}")
    print("Watermark nao e persistido localmente -- proxima execucao le o MAX confirmado direto da Bronze.")


# ---------------------------------------------------------------------------
# TRILHA 2 -- VELA EM ABERTO (sempre re-busca, nunca mexe no watermark)
# ---------------------------------------------------------------------------

def processar_vela_aberta():
    print()
    print("=" * 70)
    print("TRILHA 2 -- VELA EM ABERTO (acompanhamento ao vivo)")
    print("=" * 70)

    # Sem startTime/endTime + limit=1 -> Binance devolve a vela MAIS RECENTE,
    # que é exatamente a que está em formação (se ainda não fechou).
    raw = chamar_api({"symbol": SYMBOL, "interval": INTERVAL, "limit": 1})
    df = parse_klines(raw)

    if df.empty:
        print("Nenhum dado retornado.")
        return

    agora = datetime.now(timezone.utc)
    df["fechada"] = df["close_time"] <= agora
    df["ingest_ts_utc"] = agora.isoformat()
    df["source_symbol"] = SYMBOL

    fechada = bool(df["fechada"].iloc[0])
    linha = df.iloc[0]

    print(f"open_time:  {linha['open_time']}")
    print(f"close_time: {linha['close_time']}")
    print(f"status:     {'FECHADA' if fechada else 'EM FORMAÇÃO'}")
    print(f"open={linha['open']}  high={linha['high']}  low={linha['low']}  "
          f"close={linha['close']}  volume={linha['volume']}  trades={linha['num_trades']}")

    if fechada:
        print("\nVela já fechou -- a trilha CONFIRMADO vai assumi-la na próxima execução.")
        print("(não sobrescrevendo o arquivo de vela aberta para não gerar dado órfão)")
        return

    # Sempre sobrescreve o MESMO arquivo -- não é histórico, é "estado atual"
    pasta = LANDING_BASE / f"ingest_date={agora.date().isoformat()}"
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / "vela_em_aberto.parquet"
    df.to_parquet(arquivo, engine="pyarrow", index=False, compression="snappy")
    print(f"\nEstado atual gravado (sobrescrito) em: {arquivo}")


def main():
    processar_confirmado()
    processar_vela_aberta()


if __name__ == "__main__":
    main()
