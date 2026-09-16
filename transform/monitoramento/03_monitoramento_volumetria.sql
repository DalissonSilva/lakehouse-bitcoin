-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Monitoramento — volumetria via histórico do Delta
-- MAGIC
-- MAGIC Sem tabela de log própria: o Delta Lake já grava, a cada `MERGE` ou
-- MAGIC `CREATE OR REPLACE TABLE`, quando rodou e quantas linhas mexeu
-- MAGIC (`DESCRIBE HISTORY`). As views abaixo só leem esse histórico e agregam
-- MAGIC por dia/mês — nada para os notebooks 01 e 02 gravarem a mais.

-- COMMAND ----------

USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS controle COMMENT 'Views de monitoramento sobre o histórico Delta';

-- COMMAND ----------

-- DBTITLE 1,Execuções silver (uma linha por versão da tabela)
CREATE OR REPLACE VIEW controle.historico_silver AS
SELECT
  timestamp                                                   AS executado_em,
  operation                                                    AS operacao,
  CAST(operationMetrics['numTargetRowsInserted'] AS BIGINT)    AS linhas_inseridas,
  CAST(operationMetrics['numTargetRowsUpdated']  AS BIGINT)    AS linhas_atualizadas,
  CAST(operationMetrics['numTargetRowsDeleted']  AS BIGINT)    AS linhas_deletadas,
  CAST(operationMetrics['executionTimeMs'] AS BIGINT) / 1000.0 AS duracao_seg
FROM (DESCRIBE HISTORY workspace.silver.btc_candles_diarios)
WHERE operation = 'MERGE';

-- COMMAND ----------

-- DBTITLE 1,Execuções gold (uma linha por versão, por tabela)
CREATE OR REPLACE VIEW controle.historico_gold AS
SELECT 'btc_diario' AS tabela, timestamp AS executado_em, operation AS operacao,
       CAST(operationMetrics['numOutputRows'] AS BIGINT) AS linhas,
       CAST(operationMetrics['executionTimeMs'] AS BIGINT) / 1000.0 AS duracao_seg
FROM (DESCRIBE HISTORY workspace.gold.btc_diario)
WHERE operation IN ('CREATE OR REPLACE TABLE AS SELECT', 'CREATE TABLE AS SELECT')

UNION ALL
SELECT 'btc_mensal', timestamp, operation,
       CAST(operationMetrics['numOutputRows'] AS BIGINT),
       CAST(operationMetrics['executionTimeMs'] AS BIGINT) / 1000.0
FROM (DESCRIBE HISTORY workspace.gold.btc_mensal)
WHERE operation IN ('CREATE OR REPLACE TABLE AS SELECT', 'CREATE TABLE AS SELECT')

UNION ALL
SELECT 'btc_dia_semana', timestamp, operation,
       CAST(operationMetrics['numOutputRows'] AS BIGINT),
       CAST(operationMetrics['executionTimeMs'] AS BIGINT) / 1000.0
FROM (DESCRIBE HISTORY workspace.gold.btc_dia_semana)
WHERE operation IN ('CREATE OR REPLACE TABLE AS SELECT', 'CREATE TABLE AS SELECT')

UNION ALL
SELECT 'btc_kpis', timestamp, operation,
       CAST(operationMetrics['numOutputRows'] AS BIGINT),
       CAST(operationMetrics['executionTimeMs'] AS BIGINT) / 1000.0
FROM (DESCRIBE HISTORY workspace.gold.btc_kpis)
WHERE operation IN ('CREATE OR REPLACE TABLE AS SELECT', 'CREATE TABLE AS SELECT');

-- COMMAND ----------

-- DBTITLE 1,Volumetria por dia (silver + gold juntas)
CREATE OR REPLACE VIEW controle.volumetria_diaria AS
SELECT
  DATE(executado_em)          AS dia,
  'silver'                    AS camada,
  COUNT(*)                    AS qtd_execucoes,
  SUM(linhas_inseridas)       AS total_linhas_inseridas,
  SUM(linhas_atualizadas)     AS total_linhas_atualizadas,
  ROUND(AVG(duracao_seg), 1)  AS duracao_media_seg
FROM controle.historico_silver
GROUP BY DATE(executado_em)

UNION ALL

SELECT
  DATE(executado_em)          AS dia,
  'gold'                      AS camada,
  COUNT(*)                    AS qtd_execucoes,
  SUM(linhas)                 AS total_linhas_inseridas,
  CAST(NULL AS BIGINT)        AS total_linhas_atualizadas,
  ROUND(AVG(duracao_seg), 1)  AS duracao_media_seg
FROM controle.historico_gold
GROUP BY DATE(executado_em)

ORDER BY dia DESC, camada;

-- COMMAND ----------

-- DBTITLE 1,Volumetria por mês
CREATE OR REPLACE VIEW controle.volumetria_mensal AS
SELECT
  DATE_TRUNC('MONTH', dia)    AS mes_ref,
  camada,
  SUM(qtd_execucoes)          AS qtd_execucoes,
  SUM(total_linhas_inseridas) AS total_linhas_inseridas,
  SUM(total_linhas_atualizadas) AS total_linhas_atualizadas,
  ROUND(AVG(duracao_media_seg), 1) AS duracao_media_seg
FROM controle.volumetria_diaria
GROUP BY DATE_TRUNC('MONTH', dia), camada
ORDER BY mes_ref DESC, camada;

-- COMMAND ----------

-- DBTITLE 1,Conferência rápida
SELECT * FROM controle.volumetria_diaria LIMIT 15;

-- COMMAND ----------

-- DBTITLE 1,Alerta: dia sem nenhuma carga gold nos últimos 30 dias
SELECT d.dia AS dia_sem_carga
FROM (
  SELECT EXPLODE(SEQUENCE(DATE_SUB(CURRENT_DATE(), 30), CURRENT_DATE(), INTERVAL 1 DAY)) AS dia
) d
LEFT ANTI JOIN controle.historico_gold g ON DATE(g.executado_em) = d.dia
ORDER BY d.dia;