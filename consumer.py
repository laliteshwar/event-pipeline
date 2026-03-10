from kafka import KafkaConsumer, KafkaProducer
import json
import time
import redis
import psycopg2

r = redis.Redis(host = 'localhost', port = 6379, decode_responses=True)


dlq_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer = lambda v:json.dumps(v).encode('utf-8')
)
consumer = KafkaConsumer(
        'ecommerce-events',
        bootstrap_servers="localhost:9092",
        value_deserializer = lambda v:json.loads(v.decode('utf-8'))
    )

conn = psycopg2.connect(
        host = "localhost", port = 5432,
        database = 'events', user="sunny", password="password"
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
            r.sadd("processed_events",event_id)
            r.expire("processed_events",86400)
        except:
            dlq_producer.send('dlq-events',event)
            print("failed events / exisiting events sent to dlq events topic")
        
    
