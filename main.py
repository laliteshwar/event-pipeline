from fastapi import FastAPI
import redis

app = FastAPI()

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

@app.get("/")
def home():
    return {"status": "Pipeline is running"}

@app.get("/top-users")
def top_users():
    # Get all user keys from Redis
    keys = r.keys("user:*:count")
    
    # Build list of (user_id, count)
    user_counts = []
    for key in keys:
        user_id = key.split(":")[1]
        count = int(r.get(key))
        user_counts.append({"user_id": user_id, "count": count})
    
    # Sort by count descending
    user_counts.sort(key=lambda x: x["count"], reverse=True)
    
    # Return top 10
    return {
        "top_users": user_counts[:10],
        "total_users_tracked": len(user_counts)
    }

@app.get("/user/{user_id}")
def get_user(user_id: str):
    count = r.get(f"user:{user_id}:count")
    return {
        "user_id": user_id,
        "event_count": int(count) if count else 0
    }
