-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · Silver — BTC/USDT candles diários
-- MAGIC
-- MAGIC **Origem:** `bronze.btc_klines_1d` (ajuste o nome na célula de parâmetros)
-- MAGIC **Destino:** `silver.btc_candles_diarios` + `silver.btc_candles_rejeitados`
-- MAGIC
-- MAGIC O que esta camada faz:
-- MAGIC 1. **Deduplica** — a bronze recebe o mesmo dia em mais de uma ingestão (o candle do dia é gravado aberto e depois fechado). Fica só a versão mais recente por `símbolo + open_time`.
-- MAGIC 2. **Tipa e padroniza** — `ingest_ts_utc` chega como STRING na bronze e vira TIMESTAMP; preços viram DECIMAL; colunas em português.
-- MAGIC 3. **Valida** — regras de qualidade (preço > 0, high ≥ open/close, low ≤ open/close). Linhas reprovadas vão para a tabela de rejeitados, com o motivo.
-- MAGIC 4. **Carga incremental** com `MERGE` — um candle aberto é atualizado quando chega a versão fechada.

-- COMMAND ----------

-- DBTITLE 1,Parâmetros
USE CATALOG bitcoin;             -- catálogo do projeto
SET TIME ZONE 'UTC';            -- candles da Binance são em UTC

CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Dados limpos, deduplicados e validados';

-- COMMAND ----------

-- DBTITLE 1,Tabelas de destino
CREATE TABLE IF NOT EXISTS silver.btc_candles_diarios (
  simbolo            STRING         NOT NULL COMMENT 'Par negociado, ex.: BTCUSDT',
  data               DATE           NOT NULL COMMENT 'Dia do candle (UTC)',
  open_time          TIMESTAMP      COMMENT 'Abertura do candle (UTC)',
  close_time         TIMESTAMP      COMMENT 'Fechamento do candle (UTC)',
  preco_abertura     DECIMAL(18,2)  COMMENT 'Preço de abertura em USDT',
  preco_maximo       DECIMAL(18,2)  COMMENT 'Maior preço do dia em USDT',
  preco_minimo       DECIMAL(18,2)  COMMENT 'Menor preço do dia em USDT',
  preco_fechamento   DECIMAL(18,2)  COMMENT 'Preço de fechamento em USDT',
  volume_btc         DECIMAL(28,8)  COMMENT 'Volume negociado em BTC',
  qtd_negocios       BIGINT         COMMENT 'Número de negócios no dia',
  candle_fechado     BOOLEAN        COMMENT 'false = dia ainda em andamento (valores parciais)',
  ingest_ts_utc      TIMESTAMP      COMMENT 'Momento da ingestão na bronze',
  transformado_em    TIMESTAMP      COMMENT 'Momento em que este registro foi transformado (carga na silver)'
)
CLUSTER BY (simbolo, data)
COMMENT 'Candles diários BTC deduplicados e validados';

CREATE TABLE IF NOT EXISTS silver.btc_candles_rejeitados (
  simbolo          STRING,
  open_time        TIMESTAMP,
  motivo           STRING,
  registro         STRING  COMMENT 'Linha original em JSON',
  ingest_ts_utc    TIMESTAMP,
  rejeitado_em     TIMESTAMP
)
COMMENT 'Linhas da bronze reprovadas nas regras de qualidade';

-- COMMAND ----------

-- DBTITLE 1,Deduplicação + tipagem + regras de qualidade
CREATE OR REPLACE TEMP VIEW vw_bronze_tratada AS
WITH tipada AS (
  SELECT
    UPPER(TRIM(source_symbol))                  AS simbolo,
    CAST(open_time AS TIMESTAMP)                AS open_time,
    CAST(close_time AS TIMESTAMP)               AS close_time,
    CAST(open   AS DECIMAL(18,2))               AS preco_abertura,
    CAST(high   AS DECIMAL(18,2))               AS preco_maximo,
    CAST(low    AS DECIMAL(18,2))               AS preco_minimo,
    CAST(close  AS DECIMAL(18,2))               AS preco_fechamento,
    CAST(volume AS DECIMAL(28,8))               AS volume_btc,
    CAST(num_trades AS BIGINT)                  AS qtd_negocios,
    COALESCE(fechada, false)                    AS candle_fechado,
    TRY_TO_TIMESTAMP(ingest_ts_utc)             AS ingest_ts_utc,
    TO_JSON(STRUCT(*))                          AS registro
  FROM bronze.btc_ohlcv
),
dedup AS (
  SELECT *
  FROM tipada
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY simbolo, open_time
    ORDER BY candle_fechado DESC, ingest_ts_utc DESC   -- versão fechada e mais recente vence
  ) = 1
)
SELECT
  *,
  CASE
    WHEN simbolo IS NULL OR open_time IS NULL                     THEN 'chave nula'
    WHEN preco_abertura <= 0 OR preco_fechamento <= 0
      OR preco_maximo  <= 0 OR preco_minimo     <= 0              THEN 'preço não positivo'
    WHEN preco_maximo < GREATEST(preco_abertura, preco_fechamento) THEN 'máxima menor que abertura/fechamento'
    WHEN preco_minimo > LEAST(preco_abertura, preco_fechamento)    THEN 'mínima maior que abertura/fechamento'
    WHEN volume_btc < 0 OR qtd_negocios < 0                        THEN 'volume ou negócios negativos'
    WHEN close_time <= open_time                                   THEN 'close_time anterior ao open_time'
  END AS motivo_rejeicao
FROM dedup;

-- COMMAND ----------

-- DBTITLE 1,Grava rejeitados
INSERT INTO silver.btc_candles_rejeitados
SELECT simbolo, open_time, motivo_rejeicao, registro, ingest_ts_utc, CURRENT_TIMESTAMP()
FROM vw_bronze_tratada b
WHERE motivo_rejeicao IS NOT NULL
  AND NOT EXISTS (                       -- evita regravar o mesmo erro a cada execução
    SELECT 1 FROM silver.btc_candles_rejeitados r
    WHERE r.simbolo <=> b.simbolo
      AND r.open_time <=> b.open_time
      AND r.ingest_ts_utc <=> b.ingest_ts_utc
  );

-- COMMAND ----------

-- DBTITLE 1,MERGE incremental na silver
MERGE INTO silver.btc_candles_diarios AS t
USING (
  SELECT
    simbolo,
    TO_DATE(open_time)   AS data,
    open_time, close_time,
    preco_abertura, preco_maximo, preco_minimo, preco_fechamento,
    volume_btc, qtd_negocios, candle_fechado, ingest_ts_utc,
    CURRENT_TIMESTAMP()  AS transformado_em
  FROM vw_bronze_tratada
  WHERE motivo_rejeicao IS NULL
) AS s
ON  t.simbolo = s.simbolo
AND t.data    = s.data
WHEN MATCHED AND (
       s.ingest_ts_utc > t.ingest_ts_utc
    OR (s.candle_fechado AND NOT t.candle_fechado)
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 1,Checagens rápidas
SELECT
  simbolo,
  MIN(data)                                   AS primeiro_dia,
  MAX(data)                                   AS ultimo_dia,
  COUNT(*)                                    AS dias_carregados,
  DATEDIFF(MAX(data), MIN(data)) + 1          AS dias_esperados,
  COUNT_IF(NOT candle_fechado)                AS candles_em_aberto
FROM silver.btc_candles_diarios
GROUP BY simbolo;

-- COMMAND ----------

-- DBTITLE 1,Dias faltando na série (buracos)
SELECT d.data AS dia_faltando
FROM (
  SELECT EXPLODE(SEQUENCE(MIN(data), MAX(data), INTERVAL 1 DAY)) AS data
  FROM silver.btc_candles_diarios
) d
LEFT ANTI JOIN silver.btc_candles_diarios s ON s.data = d.data
ORDER BY d.data;