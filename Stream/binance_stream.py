import websocket
from kafka import KafkaProducer
import json
from kafka.errors import KafkaError
import signal
import sys

producer = KafkaProducer(
    bootstrap_servers="localhost:19092",   # or kafka:9092 if inside container
    value_serializer=lambda v: v.encode("utf-8")
)

topic_name = "Binance"

def shutdown(sig, frame):
    global running
    print("Shutdown signal received...")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def on_message(ws, message):
    try:
        producer.send(topic_name, value=message)
        print("Sent:", message)
    except KafkaError as e:
        print(f"Kafka error {e}")
    except Exception as e:
        print(f"UnExpected Error {e}")


def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    print("Opened connection")


def start_ws():

    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/bnbbtc@depth", 
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()



if __name__ == "__main__":
    start_ws()