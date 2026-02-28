import json
import redis
import psycopg2
from kafka import KafkaConsumer

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host='localhost', port=5432,
    database='events', user='sunny', password='password'
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50),
        item_id VARCHAR(50),
        action VARCHAR(50),
        timestamp FLOAT
    )
''')
conn.commit()

# Connect to Kafka
consumer = KafkaConsumer(
    'user-events',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Consumer started — listening for events...")

batch = []

for message in consumer:
    event = message.value
    user_id = event['user_id']

    # Increment Redis counter for this user
    r.incr(f"user:{user_id}:count")
    print(f"Processed: {user_id} | Redis count: {r.get(f'user:{user_id}:count')}")

    # Add to batch
    batch.append(event)

    # Every 100 events → save to PostgreSQL
    if len(batch) >= 100:
        cursor.executemany(
            "INSERT INTO events (user_id, item_id, action, timestamp) VALUES (%s, %s, %s, %s)",
            [(e['user_id'], e['item_id'], e['action'], e['timestamp']) for e in batch]
        )
        conn.commit()
        print(f"Saved {len(batch)} events to PostgreSQL")
        batch = []