# Extract data from API and save in Bronze
import requests
import json
from datetime import datetime
from pathlib import Path

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

DEFAULT_PARAMETERS = {
    "vs_currency" : 'usd',
    'order':'market_cap_desc',
    'per_page':100,
    'page':1,
    'sparkline' : False
}

def fetch_coins_market_data():
    response = requests.get(COINGECKO_URL,params=DEFAULT_PARAMETERS,timeout=30)
    response.raise_for_status()
    return response.json()

def save_to_bronze(data,bronze_base_path):
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

    output_dir = Path(bronze_base_path) / 'coins' / f'date={ingestion_date}'
    output_dir.mkdir(parents=True,exist_ok=True)

    output_path = output_dir / 'data.json'

    with open(output_path,'w',encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return str(output_path)

def extract_and_save(bronze_base_path):
    data = fetch_coins_market_data()
    output_path = save_to_bronze(data,bronze_base_path)
    return output_path



if __name__ == '__main__':
    path = extract_and_save("data/bronze")
    print(f"Done. Output: {path}")