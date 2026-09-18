-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Glossário — indicadores do dashboard
-- MAGIC
-- MAGIC Tabela de referência estática (não depende da bronze, não precisa rodar
-- MAGIC no Job diário) com a definição de cada indicador usado no Panorama.
-- MAGIC Alimenta a página "Glossário" do menu lateral.
-- MAGIC
-- MAGIC Para adicionar um novo indicador no futuro, basta rodar um novo `INSERT`
-- MAGIC (célula "Adicionar novo indicador" no final) — não precisa recriar a tabela.

-- COMMAND ----------

USE CATALOG bitcoin;
CREATE SCHEMA IF NOT EXISTS documentacao COMMENT 'Conteúdo de referência do dashboard: glossário, notas, changelog';

-- COMMAND ----------

-- DBTITLE 1,Tabela do glossário
CREATE OR REPLACE TABLE documentacao.glossario_indicadores (
  ordem       INT        COMMENT 'Ordem de exibição sugerida na página',
  categoria   STRING     COMMENT 'Agrupamento: Preço, Variação, Risco, Volume, Sazonalidade',
  indicador   STRING     COMMENT 'Nome do indicador como aparece no dashboard',
  definicao   STRING     COMMENT 'Explicação em linguagem simples',
  formula     STRING     COMMENT 'Como é calculado',
  coluna_gold STRING     COMMENT 'Tabela.coluna de origem, para rastreabilidade'
)
COMMENT 'Glossário de indicadores exibidos no Panorama e demais páginas';

-- COMMAND ----------

-- DBTITLE 1,Carga inicial
INSERT INTO documentacao.glossario_indicadores VALUES
(1,  'Preço',       'Último fechamento',
     'Preço de fechamento do último dia com o candle já encerrado (não conta o dia em andamento).',
     'Valor de fechamento (close) do candle diário mais recente já fechado.',
     'gold.btc_kpis.ultimo_fechamento'),

(2,  'Preço',       'Parcial hoje',
     'Preço de fechamento do candle do dia atual, que ainda está em andamento e pode mudar até o fim do dia.',
     'Valor de fechamento do candle mais recente com candle_fechado = false.',
     'gold.btc_kpis.preco_parcial'),

(3,  'Variação',    'Variação 24h',
     'Quanto o preço mudou, em %, comparado ao fechamento do dia anterior.',
     '(fechamento de hoje / fechamento de ontem - 1) × 100',
     'gold.btc_kpis.variacao_1d_pct'),

(4,  'Variação',    'Variação 30 dias',
     'Quanto o preço mudou, em %, nos últimos 30 dias corridos.',
     '(fechamento atual / fechamento de 30 dias atrás - 1) × 100',
     'gold.btc_kpis.variacao_30d_pct'),

(5,  'Variação',    'Variação no ano',
     'Quanto o preço mudou, em %, desde o último fechamento do ano anterior (31/12).',
     '(fechamento atual / fechamento de 31/12 do ano anterior - 1) × 100',
     'gold.btc_kpis.variacao_ano_pct'),

(6,  'Risco',       'Distância da máxima (Drawdown)',
     'O quanto o preço atual está abaixo do maior valor já registrado na série histórica. Sempre zero ou negativo.',
     '(fechamento atual / máxima histórica - 1) × 100',
     'gold.btc_diario.drawdown_pct'),

(7,  'Risco',       'Volatilidade 30d (anualizada)',
     'Mede o quanto o preço está "balançando" nos últimos 30 dias. Quanto maior, mais imprevisível o mercado está no momento.',
     'Desvio padrão dos retornos logarítmicos diários dos últimos 30 dias, multiplicado por raiz de 365 e por 100.',
     'gold.btc_diario.volatilidade_30d_anual_pct'),

(8,  'Preço',       'MM7 (Média Móvel de 7 dias)',
     'Média do preço de fechamento dos últimos 7 dias. Suaviza o "ruído" diário e mostra a tendência de curtíssimo prazo.',
     'Média aritmética simples do preço de fechamento dos últimos 7 dias (incluindo o dia atual).',
     'gold.btc_diario.mm7'),

(9,  'Preço',       'MM30 (Média Móvel de 30 dias)',
     'Mesma ideia da MM7, mas olhando um mês inteiro. Ajuda a ver a tendência de médio prazo.',
     'Média aritmética simples do preço de fechamento dos últimos 30 dias.',
     'gold.btc_diario.mm30'),

(10, 'Preço',       'MM200 (Média Móvel de 200 dias)',
     'Média de longo prazo, muito usada no mercado como referência entre "tendência de alta" e "tendência de baixa" estrutural.',
     'Média aritmética simples do preço de fechamento dos últimos 200 dias.',
     'gold.btc_diario.mm200'),

(11, 'Preço',       'Tendência (MM50 x MM200)',
     'Classificação simples de tendência: "Alta" quando a média de 50 dias está acima da de 200 dias, "Baixa" no caso contrário.',
     'Se MM50 > MM200 então "Alta", senão "Baixa". Só calculado após 200 dias de histórico.',
     'gold.btc_diario.tendencia_mm50_mm200'),

(12, 'Preço',       'Amplitude do dia',
     'O quanto o preço variou entre o menor e o maior valor do dia, em % sobre o preço de abertura.',
     '(máxima do dia - mínima do dia) / abertura do dia × 100',
     'gold.btc_diario.amplitude_pct'),

(13, 'Volume',      'Volume (BTC)',
     'Quantidade de bitcoins negociados no dia, na exchange de origem dos dados.',
     'Soma do volume negociado no candle diário.',
     'gold.btc_diario.volume_btc'),

(14, 'Volume',      'Volume relativo',
     'Compara o volume do dia com a média dos últimos 30 dias. Valores acima de 1,5× indicam um dia de negociação fora do padrão.',
     'Volume do dia / média móvel de 30 dias do volume.',
     'gold.btc_diario.volume_relativo'),

(15, 'Sazonalidade','Retorno mensal',
     'Quanto o preço variou, em %, entre a abertura e o fechamento de cada mês do calendário.',
     '(fechamento do mês / abertura do mês - 1) × 100',
     'gold.btc_mensal.retorno_mes_pct'),

(16, 'Sazonalidade','Retorno médio por dia da semana',
     'Média histórica de quanto o preço costuma variar em cada dia da semana (segunda, terça...), olhando toda a série.',
     'Média da variação percentual diária, agrupada por dia da semana.',
     'gold.btc_dia_semana.retorno_medio_pct');

-- COMMAND ----------

-- DBTITLE 1,Conferência
SELECT ordem, categoria, indicador, definicao
FROM documentacao.glossario_indicadores
ORDER BY ordem;

-- COMMAND ----------

-- DBTITLE 1,Adicionar novo indicador (exemplo — rode isolado, quando precisar)
-- INSERT INTO documentacao.glossario_indicadores VALUES
-- (17, 'Categoria', 'Nome do indicador', 'Definição em linguagem simples.', 'Fórmula de cálculo.', 'tabela.coluna');
EOF