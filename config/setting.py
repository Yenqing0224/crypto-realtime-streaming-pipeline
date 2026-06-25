import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = 'crypto-trades'
KAFKA_GROUP_ID = 'clickhouse-consumer-group'

# Binance
BINANCE_WS_URL = 'wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade'

# ClickHouse
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')