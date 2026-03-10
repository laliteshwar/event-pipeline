
from kafka import KafkaProducer   
import json
import time
import random
import uuid


producer= KafkaProducer(
        bootstrap_servers = 'localhost:9992',
        value_serializer = lambda v:json.dumps(v).encode("utf-8")
    )


users = ["user1","user2","user3"]
item_id = ["item1", "item2", "item3"]
item_price = [123,545,753]
item_name = ["chocolate", "toys", "laptops"]
action = ["added", "removed"]
status = ["initiated", "failed", "received"]






while True:
        
        order_placed ={

        "event_id" : str(uuid.uuid4()),
        "event" : "order_placed",
        "user_id" : random.choice(users),
        "item_id": random.choice(item_id),
        "item_name" : random.choice(item_name),
        "price" : random.choice(item_price),
        "quantity": random.randint(1,5),
        "timestamp" : time.time()
        }

        item_viewed ={

        "event_id" : str(uuid.uuid4()),
        "event" : "item_viewed",
        "user_id" : random.choice(users),
        "item_id": random.choice(item_id),
        "item_name" : random.choice(item_name),
        "timestamp" : time.time()
        }
        cart_updated ={
        "event_id" : str(uuid.uuid4()),
        "event" : "cart_updated",
        "user_id" : random.choice(users),
        "item_id": random.choice(item_id),
        "item_name" : random.choice(item_name),
        "quantity" : random.randint(1,5),
        "action": random.choice(action),
        "timestamp" : time.time()  
        }
        payment_initiated={
        "event_id" : str(uuid.uuid4()),
        "event" : "payment_initiated",
        "user_id" : random.choice(users),
        "item_id": random.choice(item_id),
        "amount": random.randint(100,50000),
        "status": random.choice(status),
        "timestamp": time.time()
        }
        
        events=[order_placed,payment_initiated,cart_updated,item_viewed]
        event = random.choice(events)
        producer.send("e-commerce", event)
        time.sleep(0.5)