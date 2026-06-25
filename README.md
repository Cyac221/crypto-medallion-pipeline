# Crypto Medallion Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.1-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
![Parquet](https://img.shields.io/badge/Storage-Parquet-50ABF1?style=flat)

End-to-end data engineering pipeline built on the **Medallion Architecture** (Bronze → Silver → Gold). Extracts real-time cryptocurrency market data from the CoinGecko public API, processes it with PySpark, and delivers business-ready KPIs — all orchestrated with Apache Airflow.

---

## Architecture

```
CoinGecko API (free, no key required)
        ↓
  [Airflow DAG — daily @ 00:30 UTC]
        ↓
┌───────────────────────────────────────────────────┐
│  BRONZE  —  Raw ingestion                         │
│  JSON as-is + ingestion_date metadata             │
│  /data/bronze/coins/date=YYYY-MM-DD/data.json     │
└───────────────────────────────────────────────────┘
        ↓  PySpark
┌───────────────────────────────────────────────────┐
│  SILVER  —  Cleaned & validated                   │
│  Type casting, null handling, deduplication       │
│  /data/silver/coins/ingestion_date=YYYY-MM-DD/    │
└───────────────────────────────────────────────────┘
        ↓  PySpark
┌───────────────────────────────────────────────────┐
│  GOLD  —  Business-ready KPIs                     │
│  top_coins_by_marketcap                           │
│  market_summary                                   │
│  volume_leaders                                   │
│  /data/gold/{table}/ingestion_date=YYYY-MM-DD/    │
└───────────────────────────────────────────────────┘
```

---

## Pipeline DAG

```
extract_from_api
      ↓
validate_bronze
      ↓
bronze_to_silver  (PySpark)
      ↓
silver_to_gold    (PySpark)
      ↓
validate_gold
```

Each task has retry logic (2 retries, 5 min delay). Validation tasks fail fast if data is missing or empty — no silent failures.

---

## Gold Layer Tables

### `top_coins_by_marketcap`
Top 10 cryptocurrencies by market capitalization per day.

| Column | Description |
|--------|-------------|
| `rank` | Market cap rank (1 = largest) |
| `coin_id` | CoinGecko unique identifier |
| `symbol` | Ticker (BTC, ETH...) |
| `current_price_usd` | Price in USD |
| `market_cap_usd` | Total market capitalization |
| `total_volume_usd` | 24h trading volume |
| `price_change_pct_24h` | Price change % last 24h |

### `market_summary`
Daily aggregate snapshot of the entire tracked market.

| Column | Description |
|--------|-------------|
| `total_market_cap_usd` | Sum of all market caps |
| `total_volume_usd` | Sum of all trading volumes |
| `avg_market_change_pct_24h` | Average price change across all coins |
| `coins_in_green` | Count of coins with positive 24h change |
| `coins_in_red` | Count of coins with negative 24h change |
| `btc_dominance_pct` | Bitcoin's share of total market cap |

### `volume_leaders`
Top 5 coins by 24h trading volume — signal of market activity and liquidity.

---

## Project Structure

```
crypto-medallion-pipeline/
│
├── dags/
│   └── crypto_pipeline.py        # Airflow DAG — pipeline orchestration
│
├── spark_jobs/
│   ├── bronze_to_silver.py       # PySpark: cleaning & validation
│   └── silver_to_gold.py         # PySpark: KPI aggregations
│
├── ingestion/
│   └── coingecko_extractor.py    # CoinGecko API extraction logic
│
├── data/
│   ├── bronze/                   # Raw JSON (gitignored)
│   ├── silver/                   # Cleaned Parquet (gitignored)
│   └── gold/                     # Business KPIs Parquet (gitignored)
│
├── docker-compose.yml            # Airflow + Spark environment
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.9.1 |
| Processing | Apache Spark 3.5 (PySpark) |
| Storage format | Apache Parquet |
| Ingestion | Python `requests` + CoinGecko REST API |
| Infrastructure | Docker Compose |
| Data source | [CoinGecko API](https://www.coingecko.com/en/api) (free tier) |

---

## Setup & Usage

### Prerequisites
- Docker + Docker Compose installed
- 4GB RAM available for containers

### 1. Clone the repository

```bash
git clone https://github.com/Cyac221/crypto-medallion-pipeline.git
cd crypto-medallion-pipeline
```

### 2. Configure environment

```bash
cp .env.example .env
# Generate a Fernet key for Airflow:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output as AIRFLOW__CORE__FERNET_KEY in .env
```

### 3. Set Airflow UID (Linux/Mac)

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 4. Start all services

```bash
docker compose up -d
```

Services:
- Airflow UI → [http://localhost:8080](http://localhost:8080) (admin / admin)
- Spark UI → [http://localhost:8081](http://localhost:8081)

### 5. Trigger the pipeline

In the Airflow UI:
1. Enable the `crypto_medallion_pipeline` DAG
2. Click **Trigger DAG** to run manually
3. Monitor task progress in the Graph view

### 6. Check outputs

```bash
# Bronze — raw JSON
ls data/bronze/coins/

# Silver — cleaned Parquet
ls data/silver/coins/

# Gold — business KPIs
ls data/gold/top_coins_by_marketcap/
ls data/gold/market_summary/
ls data/gold/volume_leaders/
```

---

## Silver Layer — Transformations Applied

| Transformation | Detail |
|----------------|--------|
| Type casting | All numeric fields cast to `DoubleType` or `LongType` |
| Null handling | `price_change_pct_24h`, `total_supply`, `high_24h`, `low_24h` filled with `0.0` |
| Corrupt records | Rows with null or negative `current_price` or `market_cap` dropped |
| Deduplication | Duplicates removed by `coin_id` + `last_updated_at` |
| Partitioning | Output partitioned by `ingestion_date` for efficient reads |

---

## Data Source

[CoinGecko REST API](https://www.coingecko.com/en/api) — `/coins/markets` endpoint.
Free tier, no API key required. Returns real-time data for top 100 cryptocurrencies by market cap.
