-- Создаем схемы/Creating schemes
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS cleaned;
CREATE DATABASE IF NOT EXISTS mart;

-- Создаем таблицы в raw (Airflow будет сюда загружать) /Create tables in raw format (Airflow will upload them here)
CREATE TABLE raw.market_data (
    id String,
    symbol String,
    name String,
    image String,
    current_price Float64,
    market_cap UInt64,
    total_volume UInt64,
    high_24h Float64,
    low_24h Float64,
    price_change_24h Float64,
    price_change_percentage_24h Float64,
    last_updated DateTime,
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id, last_updated);

CREATE TABLE raw.price_history (
    date Date,
    price Float64,
    coin_id String,
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (coin_id, date);


-- Создаем таблицы для очищенных данных и для хранения ветрин данных /Create tables for cleaned data and for storing data marts
-- CLEANED
CREATE TABLE IF NOT EXISTS cleaned.market_data (
    coin_id String,
    symbol String,
    name String,
    price_usd Float64,
    market_cap UInt64,
    volume_24h UInt64,
    high_24h Float64,
    low_24h Float64,
    change_24h_pct Float64,
    price_date Date,
    loaded_at DateTime
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (coin_id, price_date);

CREATE TABLE IF NOT EXISTS cleaned.price_history (
    coin_id String,
    date Date,
    price Float64,
    loaded_at DateTime
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (coin_id, date);

-- MART
CREATE TABLE IF NOT EXISTS mart.top_coins (
    coin_id String,
    symbol String,
    name String,
    price_usd Float64,
    market_cap UInt64,
    volume_24h UInt64,
    high_24h Float64,
    low_24h Float64,
    change_24h_pct Float64,
    price_date Date,
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY market_cap;

CREATE TABLE IF NOT EXISTS mart.portfolio_5 (
    coin_id String,
    symbol String,
    name String,
    price_usd Float64,
    market_cap UInt64,
    volume_24h UInt64,
    change_24h_pct Float64,
    price_date Date,
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY market_cap;

CREATE TABLE IF NOT EXISTS mart.price_change_pct (
    coin_id String,
    date Date,
    price Float64,
    change_pct Float64,
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (coin_id, date);

CREATE TABLE IF NOT EXISTS mart.market_overview (
    total_market_cap UInt64,
    total_volume_24h UInt64,
    total_coins UInt64,
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY loaded_at;
