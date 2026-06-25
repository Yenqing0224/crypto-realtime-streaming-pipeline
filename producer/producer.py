import json
import websocket
from confluent_kafka import Producer
from config.setting import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, BINANCE_WS_URL

# Init Kafka Producer
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'binance-producer'
}
kafka_producer = Producer(producer_config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Successfully sent to Kafka -> Topic: {msg.topic()} | Partition: [{msg.partition()}]")


def on_message(ws, message):
    raw_json = json.loads(message)


    if "data" in raw_json:
        data = raw_json["data"]
    else:
        data = raw_json
        
    # Safety check
    if data.get('e') != 'trade':
        return
        
    # Extract fields
    trade_event = {
        'event_time': data.get('E'), 
        'symbol': data.get('s'),
        'trade_id': data.get('t'),
        'price': float(data.get('p')),
        'quantity': float(data.get('q')),
        'buyer_is_maker': data.get('m')
    }
    
    # Convert formatted data to JSON string and send to Kafka
    payload = json.dumps(trade_event)
    kafka_producer.produce(
        topic=KAFKA_TOPIC, 
        value=payload, 
        key=trade_event['symbol'], 
        callback=delivery_report
    )
    
    # Serve delivery callback queue
    kafka_producer.poll(0)


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")
    kafka_producer.flush()


def on_open(ws):
    print("Successfully connected to Binance WebSocket! Starting to fetch live market data...")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()