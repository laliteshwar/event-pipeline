import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = [f"user_{i}" for i in range(1, 20)]
items = [f"item_{i}" for i in range(1, 50)]
actions = ["click", "view", "purchase"]

print("Producer started — sending events...")

while True:
    event = {
        "user_id": random.choice(users),
        "item_id": random.choice(items),
        "action": random.choice(actions),
        "timestamp": time.time()
    }
    producer.send("user-events", event)
    print(f"Sent: {event}")
    time.sleep(0.5)
