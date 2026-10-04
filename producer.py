import uuid
import json
from confluent_kafka import Producer

producer_config = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(producer_config)

order = {
    'order_id': str(uuid.uuid4()),
    "user": "maria",
    "item": "laptop",
    "quantity": 2
}

value = json.dumps(order).encode("utf-8")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        print(f"{msg.value().decode('utf-8')}")
        #print(dir(msg))

producer.produce(topic='orders', key=order['order_id'], value=value, callback=delivery_report)

producer.flush()