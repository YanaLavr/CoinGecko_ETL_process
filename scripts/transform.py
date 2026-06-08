import pandas as pd
from datetime import datetime
import numpy as np


def transform_market_data(raw_json):
    df = pd.DataFrame(raw_json)
    
    columns = ['id', 'symbol', 'name', 'image', 'current_price',
               'market_cap', 'total_volume', 'high_24h', 'low_24h',
               'price_change_24h', 'price_change_percentage_24h', 'last_updated']
    
    available_columns = [col for col in columns if col in df.columns]
    df = df[available_columns]
    
    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
    df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce').fillna(0).astype(int)
    df['total_volume'] = pd.to_numeric(df['total_volume'], errors='coerce').fillna(0).astype(int)
    df['high_24h'] = pd.to_numeric(df['high_24h'], errors='coerce').fillna(0)
    df['low_24h'] = pd.to_numeric(df['low_24h'], errors='coerce').fillna(0)
    df['price_change_24h'] = pd.to_numeric(df['price_change_24h'], errors='coerce').fillna(0)
    df['price_change_percentage_24h'] = pd.to_numeric(df['price_change_percentage_24h'], errors='coerce').fillna(0)
    
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    
    df['last_updated'] = df['last_updated'].fillna(pd.Timestamp.now())
    
    df['loaded_at'] = pd.Timestamp.now()
    
    return df


def transform_price_history(raw_json, coin_id):
    prices = raw_json['prices']
    
    df = pd.DataFrame(prices, columns=['timestamp', 'price'])
    
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['coin_id'] = str(coin_id)
    df['loaded_at'] = pd.Timestamp.now()
    
    df = df[['date', 'price', 'coin_id', 'loaded_at']]
    
    return df


def transform_multiple_histories(raw_data_dict):
    dfs = []
    for coin_id, raw_json in raw_data_dict.items():
        df = transform_price_history(raw_json, coin_id)
        dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)