import json
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import logging
from decimal import Decimal

# Setup logging
logger = logging.getLogger("CoinbaseConsumer")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# DynamoDB setup
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = 'BitcoinPrices'
try:
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'timestamp',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'  # String
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.wait_until_exists()
except dynamodb.meta.client.exceptions.ResourceInUseException:
    table = dynamodb.Table(table_name)

# Kafka Consumer setup
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='172.28.100.211:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def store_bitcoin_price(timestamp, price):
    try:
        # Convert price to Decimal
        item = {
            'timestamp': str(timestamp),
            'price': str(price)
        }
        table.put_item(Item=item)
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"Credentials error: {e}")
    except Exception as e:
        logger.error(f"Error storing data in DynamoDB: {e}")

for message in consumer:
    try:
        message_value = message.value
        timestamp = message_value.get('timestamp')
        price = message_value.get('price')
        if timestamp and price:
            store_bitcoin_price(timestamp, price)
            logger.info(f"Stored Bitcoin price: {price} at {timestamp}")
        else:
            logger.error(f"Missing timestamp or price in message: {message_value}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")
