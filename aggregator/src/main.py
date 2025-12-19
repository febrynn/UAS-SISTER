from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime
import uvicorn
from api.worker.processor.db import insert_event, get_events, get_stats, init_db

app = FastAPI()

@app.on_event("startup")
def startup_event():
    print("🚀 Initialize Database...")
    init_db()

# TAMBAHKAN INI agar healthcheck di docker-compose sukses (Status 200 OK)
@app.get("/")
def health_root():
    return {"status": "healthy"}

class EventPayload(BaseModel):
    data: dict

class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: EventPayload

@app.post("/publish")
def publish_event(events: List[Event]):
    for event in events:
        try:
            insert_event(topic=event.topic, event_id=event.event_id)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    return {"message": "Events processed"}

@app.get("/events")
def list_events(topic: str = None):
    return get_events(topic)

@app.get("/stats")
def get_statistics():
    stats_raw = get_stats()
    return [{"topic": s[0], "count": s[1]} for s in stats_raw]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)