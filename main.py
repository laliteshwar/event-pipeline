import os
from fastapi import FastAPI
from dotenv import load_dotenv
import redis
import psycopg2

app = FastAPI()
load_dotenv()

r = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), decode_responses=True)

conn = psycopg2.connect(
        host = os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_HOST"),
        database = os.getenv("POSTGRES_HOST"), user=os.getenv("POSTGRES_HOST"), password = os.getenv("POSTGRES_HOST")
    )
cursor = conn.cursor()



@app.get("/")
def home():
    return {"status": "Pipeline is running"}

@app.get("/analytics/summary")
def analytics_summary():
    total_events_type = '''SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type'''
    cursor.execute(total_events_type)
    events_by_type = cursor.fetchall()
    most_ordered='''SELECT item_name, COUNT(*) as order FROM events 
                    WHERE event_type ='order_placed' GROUP BY item_name
                    ORDER BY order DESC LIMIT 5;'''
    cursor.execute(most_ordered)
    most_orders=cursor.fetchall()
    most_viewed='''SELECT item_name, COUNT(*) as viewed FROM events
                   WHERE event_type='item_viewed' GROUP BY item_name
                   ORDER BY viewed DESC LIMIT 5'''
    cursor.execute(most_viewed)
    most_views=cursor.fetchall()
    return {
            
                "events_by_type":events_by_type,
                "most_order":most_orders,
                "most_views":most_views
        }

@app.get("/user/{user_id}")
def get_user(user_id: str):
    count = r.get(f"user:{user_id}:count")
    return {
            "user_id": user_id,
            "event_count": int(count) if count else 0
        }
