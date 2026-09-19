# Lakehouse Bitcoin

Pipeline de dados de mercado (BTC/USDT, candles diários) construído do zero:
ingestão via Binance, orquestração própria com Airflow em Docker, arquitetura
medalhão no Databricks (Unity Catalog + Delta Lake) e consumo num dashboard
Qlik Cloud com identidade visual própria.

Projeto pessoal, sem vínculo com nenhuma empresa — construído para aprender
e demonstrar engenharia de dados de ponta a ponta.

## O pipeline, de verdade

```
Binance API
    │  extração incremental, a cada hora
    ▼
Airflow (self-hosted, Docker) ── extrai → grava parquet → sobe pro Databricks
    │
    ▼
Databricks Free Edition — catálogo `bitcoin`, Unity Catalog
    │
    ├── bronze   candles brutos, como chegam da exchange
    ├── silver   deduplicados, tipados, validados (regras de qualidade)
    ├── gold     métricas de negócio: retornos, médias móveis, volatilidade,
    │            drawdown, sazonalidade
    ├── controle views de observabilidade do próprio pipeline
    └── documentacao  glossário de indicadores
    │
    ▼
Qlik Cloud — dashboard "Blockwatch", com IA generativa nativa (Qlik Answers)
```

![Ingestão rodando no Airflow](docs/images/airflow.png)
*DAG `btc_bronze_pipeline`, 25 execuções, 100% de sucesso, rodando de hora em hora.*

## Por que arquitetura medalhão, e não uma tabela só

A bronze recebe o mesmo dia mais de uma vez — a exchange grava o candle
aberto e depois fechado. A silver deduplica e valida (preço positivo,
máxima ≥ abertura/fechamento, etc.), descartando o que não passa numa
tabela de rejeitados à parte. A gold já entrega métricas prontas — médias
móveis de 7/30/50/200 dias, volatilidade anualizada, drawdown — para o
dashboard não precisar recalcular nada em tempo de consulta.

## Governança, não só pipeline

Cada tabela tem comentário de coluna, com boa parte deles gerados com
apoio de IA (Genie) e revisados manualmente — inclusive achei e corrigi
alguns que soavam genéricos demais.

![Comentários de coluna assistidos por IA](docs/images/genie_comentarios.png)

A linhagem é automática — dá pra rastrear qualquer número da gold até a
linha exata da bronze que o originou:

![Linhagem completa da tabela btc_diario](docs/images/linhagem.png)

E as tabelas são Delta gerenciado de verdade, não CSV disfarçado — com
row tracking e deletion vectors habilitados:

![Detalhes da tabela no Unity Catalog](docs/images/detalhes_tabela.png)

## Observando o próprio pipeline

Além do dashboard de negócio, montei um painel operacional nativo do
Databricks só para acompanhar a saúde da carga — volume processado por
camada, duração média, execuções por dia. Não é comum em projeto de
portfólio, mas é exatamente o tipo de coisa que se cobra em produção.

![Painel de cargas e processamento](docs/images/dashboard_cargas.png)

## O dashboard final

Identidade visual própria (Blockwatch), sem nenhum componente de
template pronto — cores, tipografia e ícones desenhados para o projeto.
O painel também usa IA generativa nativa do Qlik para responder perguntas
em linguagem natural sobre os dados.

![Dashboard Blockwatch com IA generativa](docs/images/qlik_ia.png)

## Stack

| Camada | Tecnologia |
|---|---|
| Ingestão | Python (`requests`) · API da Binance |
| Orquestração | Apache Airflow, self-hosted em Docker |
| Lakehouse | Databricks Free Edition · Delta Lake · Unity Catalog |
| Transformação | SQL (notebooks versionados no Git) |
| Governança | Unity Catalog: linhagem automática, comentários, controle de acesso |
| Observabilidade | Dashboard nativo do Databricks sobre o histórico Delta |
| Consumo | Qlik Cloud, com IA generativa nativa |
| Versionamento | Git, sincronizado direto no Workspace do Databricks |

## Estrutura do repositório

![Organização das pastas](docs/images/estrutura.png)

```
lakehouse-bitcoin/
├── dags/                  DAGs do Airflow (ingestão)
├── scripts/               scripts de extração da Binance
└── transform/
    ├── silver/            dedup, tipagem, regras de qualidade
    ├── gold/               métricas de negócio
    ├── monitoramento/      volumetria e observabilidade do pipeline
    └── documentacao/       glossário de indicadores
```

## Estado atual

O que já está rodando de ponta a ponta: ingestão incremental, três camadas
no Databricks com atualização automática por gatilho de tabela, governança
básica aplicada, e o dashboard publicado no Qlik Cloud.

Próximo passo natural, se eu continuar: cruzar com indicadores
macroeconômicos (taxa de juros, por exemplo) — já testei a viabilidade de
puxar dados do FRED direto no Databricks, mas deixei fora do escopo desta
primeira entrega para não adiar o que já estava pronto.

## Autor

Dalisson Silva · [https://www.linkedin.com/in/dalisson-silva-a01a591a7/](#) · [https://github.com/DalissonSilva](#)