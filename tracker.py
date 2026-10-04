import json
from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['orders'])

print("Consuming messages from 'orders' topic...")

while True:
    msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

    if msg is None:
        continue  # No message received, continue polling
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    value = msg.value()
    if value is None:
        continue

    value = value.decode('utf-8')
    order = json.loads(value)

    # Process the received message
    print(f"Received message: {order['quantity']} x {order['item']} for user {order['user']} with order ID {order['order_id']}")
    