import requests
import time
from datetime import datetime

BASE_URL = "https://api.coingecko.com/api/v3"
RATE_LIMIT_DELAY = 1.5 


def get_market_data(per_page=50, page=1):
    """
    Endpoint: GET /coins/markets
    """
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": False
    }
    
    url = BASE_URL + '/coins/markets'
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Код ошибки: {response.status_code}")
        return None


def get_price_history(coin_id, days=30):
    """
    Endpoint: GET /coins/{coin_id}/market_chart
    """
    params = {
        "vs_currency": "usd",
        "days": days,
        "interval": "daily"
    }
    
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Код ошибки: {response.status_code}")
        return None
        

def fetch_multiple_histories(coin_ids, days=30):
    results = {}

    for coin_id in coin_ids:
        print(f"[{datetime.now()}] Идет загрузка...{coin_id}")

        data = get_price_history(coin_id, days=days)

        if data is not None:
            results[coin_id] = data
        else:
            print(f"Ошибка загрузки {coin_id}")

        time.sleep(RATE_LIMIT_DELAY)
        
    return results