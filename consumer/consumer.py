import json
import time
import clickhouse_connect
from confluent_kafka import Consumer, KafkaError
from config.setting import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
)


# Connect to ClickHouse
print("Connecting to ClickHouse...")
ch_client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD
)
print("ClickHouse connected!")


# Init Kafka Consumer
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest' 
}
kafka_consumer = Consumer(consumer_config)
kafka_consumer.subscribe([KAFKA_TOPIC])

# Batch processing parameters
BATCH_SIZE = 1000
FLUSH_INTERVAL = 3.0


def run_ingestion_pipeline():
    batch_data = []
    last_flush_time = time.time()
    
    print(f"Started listening to Kafka topic: {KAFKA_TOPIC}")
    
    try:
        while True:
            # Poll Kafka messsages
            msg = kafka_consumer.poll(1.0)
            
            if msg is not None:
                if msg.error():
                    print(f"⚠️ Kafka error: {msg.error()}")
                    continue
                
                # Parse raw JSON data
                raw_value = msg.value().decode('utf-8')
                trade = json.loads(raw_value)
                
                row = (
                    trade['event_time'],
                    trade['symbol'],
                    trade['trade_id'],
                    trade['price'],
                    trade['quantity'],
                    1 if trade['buyer_is_maker'] else 0
                )
                batch_data.append(row)

            # Two conditions triggers batch insert
            time_elapsed = time.time() - last_flush_time
            if len(batch_data) >= BATCH_SIZE or (len(batch_data) > 0 and time_elapsed >= FLUSH_INTERVAL):
                try:
                    # Batch insert into ClickHouse
                    ch_client.insert(
                        'default.crypto_trades', 
                        batch_data, 
                        column_names=['event_time', 'symbol', 'trade_id', 'price', 'quantity', 'buyer_is_maker']
                    )
                    print(f"🚀 Successfully inserted a batch of {len(batch_data)} rows into ClickHouse.")
                except Exception as e:
                    print(f"❌ ClickHouse insert failed: {e}")
                finally:
                    # Clear batch and reset timer
                    batch_data.clear()
                    last_flush_time = time.time()
                    
    except KeyboardInterrupt:
        print("Consumer stopped manually.")
    finally:
        # Close
        kafka_consumer.close()
        print("Kafka consumer connection closed.")


if __name__ == "__main__":
    run_ingestion_pipeline()