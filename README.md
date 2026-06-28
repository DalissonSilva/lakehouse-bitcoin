# 📈 Lakehouse Bitcoin — Mercado Financeiro

> Pipeline de dados de mercado financeiro (criptomoedas, 
> commodities, índices e câmbio) construído sobre arquitetura 
> **lakehouse** com Delta Lake, orquestrado por **Airflow** 
> e transformado com **dbt**.

![Free Edition](https://img.shields.io/badge/Databricks-Free_Edition-FF3621?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?logo=delta)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow)
![S3](https://img.shields.io/badge/AWS_S3-232F3E?logo=amazons3)

---

## 🎯 Objetivo

Demonstrar uma plataforma lakehouse completa ingerindo dados 
reais de mercado financeiro via yfinance, armazenando em 
Delta Lake no S3, transformando com dbt e orquestrando 
com Airflow.

---

## 🧱 Arquitetura

---

## 📦 Stack

| Camada | Tecnologia |
|---|---|
| Ingestão | Python · yfinance |
| Orquestração | Apache Airflow (local → OCI VM) |
| Storage | AWS S3 (us-west-2) · Delta Lake |
| Processamento | Databricks Serverless · Spark |
| Transformação | dbt Core |
| Governança | Unity Catalog · lineage |
| Consumo | Qlik Sense |
| Versionamento | GitHub · Databricks Repos |

---

## 📂 Estrutura

---

## 🪜 Camadas da pirâmide

| Camada | Competência demonstrada |
|---|---|
| 3 · Data Lake | S3 · Parquet · Delta · ingestão batch |
| 4 · Lakehouse | ACID · time travel · medalhão · Unity Catalog |
| 5 · Orquestração & DataOps | Airflow · dbt · CI/CD · qualidade |

---

## 📊 Ativos monitorados

| Ativo | Ticker | Classe |
|---|---|---|
| Bitcoin | BTC-USD | Cripto |
| Ethereum | ETH-USD | Cripto |
| Petróleo WTI | CL=F | Commodity |
| Petróleo Brent | BZ=F | Commodity |
| Ouro | GC=F | Commodity |
| Prata | SI=F | Commodity |
| Ibovespa | ^BVSP | Índice |
| S&P 500 | ^GSPC | Índice |
| Dow Jones | ^DJI | Índice |
| Dólar/BRL | BRL=X | Câmbio |
| Euro/USD | EURUSD=X | Câmbio |

---

## 🚀 Como executar

```bash
# 1. clonar o repositório
git clone https://github.com/seu-usuario/lakehouse-bitcoin.git

# 2. instalar dependências dbt
cd dbt && dbt deps

# 3. subir Airflow local
cd airflow && docker compose up -d

# 4. configurar .env
cp .env.example .env
# preencher DATABRICKS_HOST, DATABRICKS_TOKEN
```

---

## 📍 Status

- [x] S3 configurado (3 buckets · us-west-2)
- [x] Databricks conectado ao S3 (External Location)
- [x] GitHub integrado ao Databricks Repos
- [ ] Bronze — ingestão yfinance
- [ ] Silver — dbt métricas
- [ ] Gold — star schema
- [ ] Airflow — orquestração
- [ ] Qlik — dashboard

---

## 👤 Autor

**Dalisson Muniz** · [LinkedIn](#) · [GitHub](#)