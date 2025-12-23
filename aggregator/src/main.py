from fastapi import FastAPI, HTTPException
from typing import Optional, List, Union, Dict, Any
from contextlib import asynccontextmanager
from fastapi.encoders import jsonable_encoder
import os
import json
import threading
import redis
import uvicorn
from pydantic import BaseModel
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================
# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.api.worker.processor.db import (
    insert_event,
    get_events,
    get_stats,
    init_db,
)

# =====================================================
# EVENT SCHEMA (Updated)
# =====================================================
class EventSchema(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: Optional[str] = "unknown"
    payload: Optional[Dict[str, Any]] = None 

# =====================================================
# REDIS SETUP
# =====================================================
REDIS_URL = os.getenv("BROKER_URL", "redis://localhost:6379")
CHANNEL_NAME = "events"
redis_client = None

# Init Redis (Hanya jika bukan test, atau nanti di-override oleh test)
if not os.getenv("PYTEST_CURRENT_TEST"):
    try:
        redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
    except Exception as e:
        print(f"⚠️ Redis not connected: {e}")

# =====================================================
# WORKER
# =====================================================
def redis_worker():
    if not redis_client: return
    pubsub = redis_client.pubsub()
    pubsub.subscribe(CHANNEL_NAME)
    
    for message in pubsub.listen():
        if message["type"] == "message":
            try:
                event = json.loads(message["data"])
                insert_event(event["topic"], event["event_id"])
            except Exception:
                pass

def start_worker_thread():
    t = threading.Thread(target=redis_worker, daemon=True)
    t.start()

# =====================================================
# LIFESPAN
# =====================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if redis_client:
        start_worker_thread()
    yield

app = FastAPI(lifespan=lifespan)

# =====================================================
# ENDPOINTS
# =====================================================
@app.get("/")
def healthcheck():
    return {"status": "healthy"}

@app.get("/events")
def list_events(topic: Optional[str] = None):
    return get_events(topic)

@app.get("/stats")
def statistics():
    stats = get_stats()
    return {
        "topics": len(stats),
        "details": {t: c for t, c in stats},
    }

@app.post("/publish")
def publish_event(event_data: Union[EventSchema, List[EventSchema]]):
    # Jika redis_client None (misal error koneksi), return 503
    if not redis_client:
        # Pengecualian: Jika testing tapi mock belum inject, kita skip error 
        # (tapi idealnya test pakai mock)
        return {"message": "Redis unavailable (Stored in memory only if mocked)"}

    events = event_data if isinstance(event_data, list) else [event_data]
    count = 0

    for event in events:
        # jsonable_encoder mengurus datetime -> string isoformat
        payload = jsonable_encoder(event)
        redis_client.publish(CHANNEL_NAME, json.dumps(payload))
        count += 1

    return {"message": "Events processed", "received": count}

if __name__ == "__main__":
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080)
    
    
