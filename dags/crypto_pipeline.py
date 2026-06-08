from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

import sys
sys.path.append('/opt/airflow/scripts')

from extract import get_market_data, fetch_multiple_histories
from transform import transform_market_data, transform_multiple_histories

COINS = ['bitcoin', 'ethereum', 'solana', 'cardano', 'polkadot']

default_args = {
    'owner': 'default',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_etl',
    default_args=default_args,
    description='ETL pipeline for crypto data',
    schedule='0 9 * * *',
    catchup=False,
) as dag:
    
    def extract_market():
        data = get_market_data(per_page=50)
        return data
    
    def extract_history():
        data = fetch_multiple_histories(COINS, days=30)
        return data
    
    def transform_and_load(**context):
        market_data = context['ti'].xcom_pull(task_ids='extract_market')
        history_data = context['ti'].xcom_pull(task_ids='extract_history')
        
        df_market = transform_market_data(market_data)
        df_history = transform_multiple_histories(history_data)
        
        # Конвертируем pandas Timestamp в datetime
        for col in df_market.columns:
            if pd.api.types.is_datetime64_any_dtype(df_market[col]):
                df_market[col] = df_market[col].apply(
                    lambda x: x.to_pydatetime() if pd.notna(x) else datetime.now()
                )
        
        for col in df_history.columns:
            if pd.api.types.is_datetime64_any_dtype(df_history[col]):
                df_history[col] = df_history[col].apply(
                    lambda x: x.to_pydatetime() if pd.notna(x) else datetime.now()
                )
        
        from clickhouse_driver import Client
        client = Client(
            host='clickhouse',
            port=9000,
            user='default',
            password='password'
        )
        

        # 1. ЗАГРУЗКА В RAW (INSERT — накапливаем историю) / history data

        
        market_records = [tuple(row) for row in df_market.values]
        history_records = [tuple(row) for row in df_history.values]
        
        client.execute(
            'INSERT INTO raw.market_data (id, symbol, name, image, current_price, market_cap, total_volume, high_24h, low_24h, price_change_24h, price_change_percentage_24h, last_updated, loaded_at) VALUES',
            market_records
        )
        
        client.execute(
            'INSERT INTO raw.price_history (date, price, coin_id, loaded_at) VALUES',
            history_records
        )
        
        print(f"RAW загружено: {len(market_records)} рыночных, {len(history_records)} исторических")
        

        # 2. ОЧИСТКА В CLEANED (DELETE за сегодня + INSERT) / cleaned data

        
        # Удаляем данные за сегодня (если DAG запущен повторно)
        client.execute('''
            ALTER TABLE cleaned.market_data
            DELETE WHERE price_date = today()
        ''')
        
        client.execute('''
            ALTER TABLE cleaned.price_history
            DELETE WHERE date = today()
        ''')
        
        # Вставляем свежие очищенные данные
        client.execute('''
            INSERT INTO cleaned.market_data (coin_id, symbol, name, price_usd, market_cap, volume_24h, high_24h, low_24h, change_24h_pct, price_date, loaded_at)
            SELECT
                id AS coin_id,
                symbol,
                name,
                current_price AS price_usd,
                market_cap,
                total_volume AS volume_24h,
                high_24h,
                low_24h,
                price_change_percentage_24h AS change_24h_pct,
                toDate(last_updated) AS price_date,
                loaded_at
            FROM raw.market_data
            WHERE toDate(last_updated) = today()
        ''')
        
        client.execute('''
            INSERT INTO cleaned.price_history (coin_id, date, price, loaded_at)
            SELECT
                coin_id,
                date,
                price,
                loaded_at
            FROM raw.price_history
            WHERE date = today()
        ''')
        
        print("CLEANED обновлено")
        

        # 3. ВИТРИНЫ В MART (TRUNCATE + INSERT — пересчитываем) / data showcases

        
        # --- top_coins ---
        client.execute('TRUNCATE TABLE mart.top_coins')
        client.execute('''
            INSERT INTO mart.top_coins (coin_id, symbol, name, price_usd, market_cap, volume_24h, high_24h, low_24h, change_24h_pct, price_date, loaded_at)
            SELECT
                coin_id,
                symbol,
                name,
                price_usd,
                market_cap,
                volume_24h,
                high_24h,
                low_24h,
                change_24h_pct,
                price_date,
                loaded_at
            FROM cleaned.market_data
            WHERE (coin_id, price_date) IN (
                SELECT coin_id, MAX(price_date)
                FROM cleaned.market_data
                GROUP BY coin_id
            )
            ORDER BY market_cap DESC
            LIMIT 10
        ''')
        
        # --- top 5_coins ---
        client.execute('TRUNCATE TABLE mart.portfolio_5')
        client.execute('''
            INSERT INTO mart.portfolio_5 (coin_id, symbol, name, price_usd, market_cap, volume_24h, change_24h_pct, price_date, loaded_at)
            SELECT
                coin_id,
                symbol,
                name,
                price_usd,
                market_cap,
                volume_24h,
                change_24h_pct,
                price_date,
                loaded_at
            FROM cleaned.market_data
            WHERE coin_id IN ('bitcoin', 'ethereum', 'solana', 'cardano', 'polkadot')
              AND (coin_id, price_date) IN (
                  SELECT coin_id, MAX(price_date)
                  FROM cleaned.market_data
                  GROUP BY coin_id
              )
            ORDER BY market_cap DESC
        ''')
        
        # --- price_change_pct ---
        client.execute('TRUNCATE TABLE mart.price_change_pct')
        client.execute('''
            INSERT INTO mart.price_change_pct (coin_id, date, price, change_pct, loaded_at)
            SELECT
                coin_id,
                date,
                price,
                (price / min(price) OVER (PARTITION BY coin_id)) - 1 AS change_pct,
                now() AS loaded_at
            FROM (
                SELECT
                    coin_id,
                    date,
                    price,
                    first_value(price) OVER (PARTITION BY coin_id ORDER BY date) AS first_price
                FROM cleaned.price_history
            )
        ''')
        
        # --- market_overview ---
        client.execute('TRUNCATE TABLE mart.market_overview')
        client.execute('''
            INSERT INTO mart.market_overview (total_market_cap, total_volume_24h, total_coins, loaded_at)
            SELECT
                SUM(market_cap) AS total_market_cap,
                SUM(volume_24h) AS total_volume_24h,
                COUNT(DISTINCT coin_id) AS total_coins,
                now() AS loaded_at
            FROM cleaned.market_data
            WHERE price_date = today()
        ''')
        
        print("Витрины MART обновлены!")
    
    t1 = PythonOperator(
        task_id='extract_market',
        python_callable=extract_market
    )
    
    t2 = PythonOperator(
        task_id='extract_history',
        python_callable=extract_history
    )
    
    t3 = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load
    )
    
    [t1, t2] >> t3