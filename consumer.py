import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
import json
import time
import redis
import psycopg2

load_dotenv()

r = redis.Redis(host = os.getenv("REDIS_HOST"), port = int(os.getenv("REDIS_PORT")), decode_responses=True)


dlq_producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer = lambda v:json.dumps(v).encode('utf-8')
)
consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        value_deserializer = lambda v:json.loads(v.decode('utf-8'))
    )

conn = psycopg2.connect(
        host = os.getenv("POSTGRES_HOST"), port = int(os.getenv("POSTGRES_PORT")),
        database = os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD")
    )
cursor = conn.cursor()

cursor.execute('''CREATE TABLE if NOT EXISTS events(
        id SERIAL PRIMARY KEY,
        event_id VARCHAR(100) UNIQUE,
        event_type VARCHAR(50),
        user_id VARCHAR(50),
        item_id VARCHAR(50),
        item_name VARCHAR(100),
        price FLOAT,
        quantity INT,
        amount FLOAT,
        status VARCHAR(50),
        timestamp FLOAT
    )
    '''
    )
conn.commit()


for message in consumer:
    event = message.value
    event_id = event['event_id']

    if(r.sismember("processed_events",event_id)):
        continue
    else:
        try:
            event_id = event['event_id']
            event_type = event['event']
            user_id = event['user_id']
            item_id = event.get('item_id')
            item_name = event.get('item_name')
            price = event.get('price')
            quantity = event.get('quantity')
            amount = event.get('amount')
            status = event.get('status')
            timestamp = event['timestamp']
            cursor.execute('''INSERT into events(
                           event_id, event_type, user_id,item_id,item_name, price,quantity, amount, status,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(event_id, event_type, user_id,item_id,item_name, price,quantity, amount, status,timestamp))
            conn.commit()
            r.incr(f"user:{user_id}:count")
            r.sadd("processed_events",event_id)
            r.expire(event_id,86400)
        except Exception as e:
            dlq_producer.send(os.getenv("KAFKA_DLQ_TOPIC"),event)
            print("failed events / exisiting events sent to dlq events topic")
        
    
