from fastapi import FastAPI
import redis
import psycopg2

app = FastAPI()

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

conn = psycopg2.connect(
        host = "localhost", port=5432,
        database = 'events', user='sunny', password = 'password'
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
                    WHERE event_type ="order_placed" GROUP BY item_name
                    ORDER BY order DESC LIMIT 5;'''
    cursor.execute(most_ordered)
    most_orders=cursor.fetchall()
    most_viewed='''SELECT item_name, COUNT(*) as viewed FROM events
                   WHERE event_type="item_viewed" GROUP BY item_name
                   ORDER BY viewed DESC LIMIT 5'''
    cursor.execute(most_viewed)
    most_views=cursor.fetchall()
    return {
            
                events_by_type,
                most_orders,
                most_views
        }

@app.get("/user/{user_id}")
def get_user(user_id: str):
    count = r.get(f"user:{user_id}:count")
    return {
            "user_id": user_id,
            "event_count": int(count) if count else 0
        }
