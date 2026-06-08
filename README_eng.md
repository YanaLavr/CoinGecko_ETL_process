# CoinGecko ETL Dashboard

## Project Overview

End-to-end ETL pipeline for cryptocurrency market analysis. The project automatically collects data from the CoinGecko API, processes it through Apache Airflow, stores it in ClickHouse with a three-layer data architecture, and visualizes it in an Apache Superset dashboard.

**Result:** an interactive dashboard with key market indicators, top-10 coins by market capitalization, 5-coin portfolio dynamics, and a detailed data table.

---

## Architecture
CoinGecko API → Airflow (ETL) → ClickHouse (data warehouse) → Superset (dashboard)


### Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Data Source | CoinGecko API | Cryptocurrency market data |
| Orchestration | Apache Airflow | ETL process automation |
| Data Warehouse | ClickHouse | Column-oriented analytical database |
| Visualization | Apache Superset | Interactive dashboards |
| Languages | Python, SQL | Extraction, transformation, queries |

---

## Data Pipeline

### 1. Extract

- `get_market_data()` — fetches top-49 coins by market capitalization (price, volume, 24h change)
- `fetch_multiple_histories()` — fetches 30-day price history for 5 selected coins: Bitcoin, Ethereum, Solana, Cardano, Polkadot

### 2. Transform

- Data type conversion (numbers, dates)
- Missing value handling (fillna)
- Technical field addition (`loaded_at`)

### 3. Load

Three-layer data warehouse architecture:

| Layer | Tables | ClickHouse Engine | Purpose |
|-------|--------|-------------------|---------|
| **raw** | `market_data`, `price_history` | MergeTree | Raw API data, full load history |
| **cleaned** | `market_data`, `price_history` | ReplacingMergeTree | Cleaned data with deduplication by `loaded_at` |
| **mart** | `top_coins`, `portfolio_5`, `price_change_pct`, `market_overview` | MergeTree | Dashboard-ready data marts, recalculated daily |

---

## Dashboard

### Structure

| Section | Chart | Data |
|---------|-------|------|
| KPIs | 3 Big Numbers | Total market capitalization, 24h trading volume, number of coins |
| Market | Bar Chart | Top-10 coins by market capitalization |
| Portfolio | Line Chart | 5-coin price dynamics over 30 days (percentage from start date) |
| Details | Table | Price, market cap, volume, 24h change for 5 coins |

### Filters

- Date range selection for trend analysis
- Individual coin selection from portfolio

---

## How to Run

### Requirements

- Docker
- Docker Compose

### Dependencies

Custom Docker image includes:
- `clickhouse-driver` — ClickHouse connection
- `pandas` — Data transformation
- `requests` — API calls

Built via `Dockerfile.airflow`:
```dockerfile
FROM apache/airflow:3.2.2
RUN pip install clickhouse-driver pandas requests

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/YanaLavr/CoinGecko_ETL_process.git
cd CoinGecko_ETL_process

# 2. Start infrastructure
docker-compose up -d

# 3. Create tables in ClickHouse
# Connect to ClickHouse and run create_tables.sql
docker exec -it clickhouse clickhouse-client
# Paste contents of create_tables.sql

# 4. Open Airflow UI
# http://localhost:8080
# Login: airflow / Password: password is auto-generated - check logs
# Find DAG crypto_etl, trigger manually or wait for schedule

# 5. Open Superset
# http://localhost:8088
# Connect to ClickHouse (mart schema)
# Create charts from mart.top_coins, mart.portfolio_5, etc.

# DAG Schedule
Runs daily at 09:00 UTC: 0 9 * * *

#Project Structure
CoinGecko_ETL_process/
├── docker-compose.yml          # Infrastructure
├── create_tables.sql           # Schema and table creation
├── README.md                   # Project documentation
├── dags/
│   └── crypto_pipeline.py      # Airflow DAG
├── scripts/
│   ├── extract.py              # CoinGecko API requests
│   └── transform.py            # Data cleaning with pandas
└── screenshots/
    └── dashboard.png           # Dashboard screenshot

#Architecture Decisions
| Decision                              | Why                                                                    |
|---------------------------------------|------------------------------------------------------------------------|
| 3-layer architecture (raw/cleaned/mart) | Data loss protection, recalculation ability, dashboard performance   |
| ReplacingMergeTree in cleaned         | Automatic deduplication on repeated loads                              |
| ClickHouse                            | Columnar storage = fast analytical queries                             |
| Airflow DAG                           | Scheduled execution, no manual intervention needed                     |
