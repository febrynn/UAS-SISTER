from pydantic import BaseModel
from typing import Dict, Any, List
from datetime import datetime


class EventPayload(BaseModel):
    """
    Isi payload bebas, sesuai request kamu
    """
    data: Dict[str, Any]


class EventSchema(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: EventPayload


class PublishResponse(BaseModel):
    message: str


class EventResponse(BaseModel):
    id: int
    topic: str
    event_id: str
    timestamp: datetime


class StatsResponse(BaseModel):
    topics: int
    details: Dict[str, int]
