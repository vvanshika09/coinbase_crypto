import requests
import json
from kafka import KafkaProducer
import time
from datetime import datetime, timezone
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
import matplotlib.pyplot as plt

# Setup logging
logger = logging.getLogger("CoinbaseProducer")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler("coinbase_producer.log", maxBytes=2000000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def fetch_historical_prices():
    url = "https://api.coinbase.com/v2/prices/BTC-USD/historic?days=50"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        prices = data['data']['prices']
        return prices
    except requests.RequestException as e:
        logger.error(f"Failed to fetch historical prices: {e}")
        return None

def main():
    producer = KafkaProducer(bootstrap_servers='172.28.100.211:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    historical_prices = fetch_historical_prices()

    if historical_prices is not None:
        for price_data in historical_prices:
            timestamp = int(price_data['time'])
            timestamp_readable = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            price = float(price_data['price'])
            message = {'timestamp': timestamp_readable, 'price': price}
            try:
                producer.send('crypto_prices', value=message)
                logger.info(f"Sent message: {message}")
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
    else:
        logger.error("No historical prices fetched.")

    time.sleep(3600)  # Sleep for an hour before running again

if __name__ == "__main__":
    main()
