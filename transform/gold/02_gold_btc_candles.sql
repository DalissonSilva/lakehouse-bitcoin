-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Gold — BTC/USDT pronto para o Qlik
-- MAGIC
-- MAGIC **Origem:** `silver.btc_candles_diarios`
-- MAGIC
-- MAGIC | Tabela gold | Grão | Alimenta no dashboard |
-- MAGIC |---|---|---|
-- MAGIC | `gold.btc_diario` | 1 linha por dia | Candles + médias móveis, volume, drawdown, volatilidade |
-- MAGIC | `gold.btc_mensal` | 1 linha por mês | Heatmap de retorno mensal, ranking de meses |
-- MAGIC | `gold.btc_dia_semana` | 1 linha por dia da semana | Sazonalidade semanal |
-- MAGIC | `gold.btc_kpis` | 1 linha por símbolo | Cartões de KPI do Panorama |
-- MAGIC
-- MAGIC Só entram candles **fechados**. O dia em andamento aparece apenas como `preco_parcial` em `gold.btc_kpis`.
-- MAGIC Como o histórico diário é pequeno (~3,3 mil linhas desde 2017), as tabelas são recriadas inteiras a cada execução — simples e barato no serverless.

-- COMMAND ----------

USE CATALOG workspace;
SET TIME ZONE 'UTC';
CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Métricas de negócio prontas para consumo';

-- COMMAND ----------

-- DBTITLE 1,gold.btc_diario — fato diário com indicadores
CREATE OR REPLACE TABLE gold.btc_diario
CLUSTER BY (simbolo, data)
COMMENT 'Candle diário BTC com retornos, médias móveis, volatilidade e drawdown'
AS
WITH base AS (
  SELECT
    simbolo, data,
    CAST(preco_abertura   AS DOUBLE) AS preco_abertura,
    CAST(preco_maximo     AS DOUBLE) AS preco_maximo,
    CAST(preco_minimo     AS DOUBLE) AS preco_minimo,
    CAST(preco_fechamento AS DOUBLE) AS preco_fechamento,
    CAST(volume_btc       AS DOUBLE) AS volume_btc,
    qtd_negocios,
    LAG(CAST(preco_fechamento AS DOUBLE)) OVER (PARTITION BY simbolo ORDER BY data) AS fechamento_anterior,
    ROW_NUMBER() OVER (PARTITION BY simbolo ORDER BY data)                          AS n_dia
  FROM silver.btc_candles_diarios
  WHERE candle_fechado
),
retornos AS (
  SELECT
    *,
    preco_fechamento - fechamento_anterior                                  AS variacao_abs,
    (preco_fechamento / fechamento_anterior - 1) * 100                      AS variacao_pct,
    LN(preco_fechamento / fechamento_anterior)                              AS retorno_log,
    (preco_maximo - preco_minimo) / preco_abertura * 100                    AS amplitude_pct,
    volume_btc * (preco_maximo + preco_minimo + preco_fechamento) / 3       AS volume_usdt_estimado
  FROM base
),
janelas AS (
  SELECT
    *,
    AVG(preco_fechamento) OVER (PARTITION BY simbolo ORDER BY data ROWS BETWEEN 6   PRECEDING AND CURRENT ROW) AS mm7_raw,
    AVG(preco_fechamento) OVER (PARTITION BY simbolo ORDER BY data ROWS BETWEEN 29  PRECEDING AND CURRENT ROW) AS mm30_raw,
    AVG(preco_fechamento) OVER (PARTITION BY simbolo ORDER BY data ROWS BETWEEN 49  PRECEDING AND CURRENT ROW) AS mm50_raw,
    AVG(preco_fechamento) OVER (PARTITION BY simbolo ORDER BY data ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS mm200_raw,
    STDDEV(retorno_log)   OVER (PARTITION BY simbolo ORDER BY data ROWS BETWEEN 29  PRECEDING AND CURRENT ROW) AS desvio_30d,
    MAX(preco_fechamento) OVER (PARTITION BY simbolo ORDER BY data ROWS UNBOUNDED PRECEDING)                   AS maximo_historico_fech,
    AVG(volume_btc)       OVER (PARTITION BY simbolo ORDER BY data ROWS BETWEEN 29  PRECEDING AND CURRENT ROW) AS volume_mm30
  FROM retornos
)
SELECT
  -- chaves e calendário
  simbolo,
  data,
  YEAR(data)                                      AS ano,
  QUARTER(data)                                   AS trimestre,
  MONTH(data)                                     AS mes,
  DATE_FORMAT(data, 'yyyy-MM')                    AS ano_mes,
  DAYOFWEEK(data)                                 AS dia_semana_num,      -- 1 = domingo
  CASE DAYOFWEEK(data)
    WHEN 1 THEN 'Dom' WHEN 2 THEN 'Seg' WHEN 3 THEN 'Ter' WHEN 4 THEN 'Qua'
    WHEN 5 THEN 'Qui' WHEN 6 THEN 'Sex' ELSE 'Sáb' END AS dia_semana,

  -- OHLCV
  preco_abertura, preco_maximo, preco_minimo, preco_fechamento,
  volume_btc, volume_usdt_estimado, qtd_negocios,
  volume_btc / NULLIF(qtd_negocios, 0)            AS btc_por_negocio,

  -- retornos
  fechamento_anterior,
  ROUND(variacao_abs, 2)                          AS variacao_abs,
  ROUND(variacao_pct, 4)                          AS variacao_pct,
  retorno_log,
  ROUND(amplitude_pct, 4)                         AS amplitude_pct,
  CASE WHEN preco_fechamento >= preco_abertura THEN 'Alta' ELSE 'Baixa' END AS tipo_candle,

  -- médias móveis (nulas até existir histórico suficiente)
  ROUND(mm7_raw, 2)                               AS mm7,
  CASE WHEN n_dia >= 30  THEN ROUND(mm30_raw, 2)  END AS mm30,
  CASE WHEN n_dia >= 50  THEN ROUND(mm50_raw, 2)  END AS mm50,
  CASE WHEN n_dia >= 200 THEN ROUND(mm200_raw, 2) END AS mm200,
  CASE
    WHEN n_dia < 200            THEN NULL
    WHEN mm50_raw > mm200_raw   THEN 'Alta'
    ELSE 'Baixa'
  END                                             AS tendencia_mm50_mm200,

  -- risco
  CASE WHEN n_dia >= 31 THEN ROUND(desvio_30d * SQRT(365) * 100, 2) END AS volatilidade_30d_anual_pct,
  ROUND(maximo_historico_fech, 2)                 AS maximo_historico_fech,
  ROUND((preco_fechamento / maximo_historico_fech - 1) * 100, 2) AS drawdown_pct,

  -- liquidez
  ROUND(volume_mm30, 4)                           AS volume_mm30,
  ROUND(volume_btc / NULLIF(volume_mm30, 0), 2)   AS volume_relativo,   -- >1,5 = dia de volume atípico

  CURRENT_TIMESTAMP()                             AS transformado_em
FROM janelas;

-- COMMAND ----------

-- DBTITLE 1,gold.btc_mensal — resumo por mês
CREATE OR REPLACE TABLE gold.btc_mensal
COMMENT 'Resumo mensal BTC: retorno, extremos, volume e dias de alta/baixa'
AS
SELECT
  simbolo,
  CAST(DATE_TRUNC('MONTH', data) AS DATE)                      AS mes_ref,
  ano,
  mes,
  ano_mes,
  MIN_BY(preco_abertura, data)                                 AS abertura_mes,
  MAX_BY(preco_fechamento, data)                               AS fechamento_mes,
  MAX(preco_maximo)                                            AS maxima_mes,
  MIN(preco_minimo)                                            AS minima_mes,
  ROUND((MAX_BY(preco_fechamento, data) / MIN_BY(preco_abertura, data) - 1) * 100, 2) AS retorno_mes_pct,
  ROUND((MAX(preco_maximo) - MIN(preco_minimo)) / MIN_BY(preco_abertura, data) * 100, 2) AS amplitude_mes_pct,
  SUM(volume_btc)                                              AS volume_btc,
  SUM(volume_usdt_estimado)                                    AS volume_usdt_estimado,
  SUM(qtd_negocios)                                            AS qtd_negocios,
  COUNT(*)                                                     AS dias,
  COUNT_IF(variacao_pct > 0)                                   AS dias_alta,
  COUNT_IF(variacao_pct < 0)                                   AS dias_baixa,
  ROUND(STDDEV(retorno_log) * SQRT(365) * 100, 2)              AS volatilidade_anual_pct,
  MIN(drawdown_pct)                                            AS pior_drawdown_pct
FROM gold.btc_diario
GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,gold.btc_dia_semana — sazonalidade semanal
CREATE OR REPLACE TABLE gold.btc_dia_semana
COMMENT 'Comportamento médio do BTC por dia da semana (histórico completo)'
AS
SELECT
  simbolo,
  dia_semana_num,
  dia_semana,
  COUNT(*)                                          AS dias,
  ROUND(AVG(variacao_pct), 3)                       AS retorno_medio_pct,
  ROUND(PERCENTILE(variacao_pct, 0.5), 3)           AS retorno_mediano_pct,
  ROUND(COUNT_IF(variacao_pct > 0) / COUNT(*) * 100, 1) AS pct_dias_alta,
  ROUND(AVG(amplitude_pct), 3)                      AS amplitude_media_pct,
  ROUND(AVG(volume_btc), 2)                         AS volume_medio_btc
FROM gold.btc_diario
WHERE variacao_pct IS NOT NULL
GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,gold.btc_kpis — cartões do Panorama
CREATE OR REPLACE TABLE gold.btc_kpis
COMMENT 'Snapshot com os indicadores de topo do dashboard'
AS
WITH ref AS (
  SELECT simbolo, MAX(data) AS dt FROM gold.btc_diario GROUP BY simbolo
),
d AS (
  SELECT g.*, r.dt FROM gold.btc_diario g JOIN ref r USING (simbolo)
),
kpi AS (
  SELECT
    simbolo,
    MAX(dt)                                                            AS data_ultimo_fechamento,
    MAX_BY(preco_fechamento, data)                                     AS ultimo_fechamento,
    MAX_BY(variacao_pct, data)                                         AS variacao_1d_pct,
    MAX_BY(preco_fechamento, data) FILTER (WHERE data <= DATE_SUB(dt, 7))    AS fech_7d,
    MAX_BY(preco_fechamento, data) FILTER (WHERE data <= DATE_SUB(dt, 30))   AS fech_30d,
    MAX_BY(preco_fechamento, data) FILTER (WHERE data <= DATE_SUB(dt, 365))  AS fech_365d,
    MAX_BY(preco_fechamento, data) FILTER (WHERE data <  MAKE_DATE(YEAR(dt), 1, 1)) AS fech_ano_anterior,
    MAX(preco_maximo)                                                  AS maxima_historica,
    MAX_BY(data, preco_maximo)                                         AS data_maxima_historica,
    MAX(preco_maximo) FILTER (WHERE data > DATE_SUB(dt, 365))          AS maxima_52s,
    MIN(preco_minimo) FILTER (WHERE data > DATE_SUB(dt, 365))          AS minima_52s,
    MAX_BY(drawdown_pct, data)                                         AS drawdown_atual_pct,
    MAX_BY(volatilidade_30d_anual_pct, data)                           AS volatilidade_30d_anual_pct,
    MAX_BY(mm50, data)                                                 AS mm50,
    MAX_BY(mm200, data)                                                AS mm200,
    MAX_BY(tendencia_mm50_mm200, data)                                 AS tendencia_mm50_mm200,
    AVG(volume_btc) FILTER (WHERE data > DATE_SUB(dt, 30))             AS volume_medio_30d
  FROM d
  GROUP BY simbolo
),
parcial AS (   -- candle do dia ainda aberto (vem da silver)
  SELECT simbolo,
         MAX_BY(preco_fechamento, data) AS preco_parcial,
         MAX_BY(ingest_ts_utc, data)    AS preco_parcial_em
  FROM silver.btc_candles_diarios
  WHERE NOT candle_fechado
  GROUP BY simbolo
)
SELECT
  k.simbolo,
  k.data_ultimo_fechamento,
  k.ultimo_fechamento,
  ROUND(k.variacao_1d_pct, 2)                                               AS variacao_1d_pct,
  ROUND((k.ultimo_fechamento / k.fech_7d   - 1) * 100, 2)                   AS variacao_7d_pct,
  ROUND((k.ultimo_fechamento / k.fech_30d  - 1) * 100, 2)                   AS variacao_30d_pct,
  ROUND((k.ultimo_fechamento / k.fech_365d - 1) * 100, 2)                   AS variacao_12m_pct,
  ROUND((k.ultimo_fechamento / k.fech_ano_anterior - 1) * 100, 2)           AS variacao_ano_pct,
  k.maxima_historica, k.data_maxima_historica,
  k.maxima_52s, k.minima_52s,
  k.drawdown_atual_pct,
  k.volatilidade_30d_anual_pct,
  k.mm50, k.mm200, k.tendencia_mm50_mm200,
  ROUND(k.volume_medio_30d, 2)                                              AS volume_medio_30d,
  CAST(p.preco_parcial AS DOUBLE)                                           AS preco_parcial,
  p.preco_parcial_em,
  CURRENT_TIMESTAMP()                                                       AS transformado_em
FROM kpi k
LEFT JOIN parcial p USING (simbolo);

-- COMMAND ----------

-- DBTITLE 1,Conferência
SELECT * FROM gold.btc_kpis;

-- COMMAND ----------

SELECT data, preco_fechamento, variacao_pct, mm7, mm30, mm200, volatilidade_30d_anual_pct, drawdown_pct, volume_relativo
FROM gold.btc_diario
ORDER BY data DESC
LIMIT 15;