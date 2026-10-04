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

producer.produce(topic='orders', key=order['order_id'], value=value)

producer.flush()