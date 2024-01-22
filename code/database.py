from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis

app = FastAPI()

class Item(BaseModel):
    key: str
    value: str

# Connect to the Redis server
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

@app.post("/put_data/")
def create_item(item: Item):
    # Store key-value pair in Redis
    redis_client.set(item.key, item.value)
    return {"key": item.key, "value": item.value}

@app.get("/get_data/{key}")
def read_item(key: str):
    # Retrieve value from Redis based on key
    value = redis_client.get(key)
    print(value)

    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")

    return {"key": key, "value": value}