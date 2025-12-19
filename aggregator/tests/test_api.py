from fastapi.testclient import TestClient
from src.main import app
import pytest

client = TestClient(app)

@pytest.fixture
def sample_event():
    return {
        "topic": "test_topic",
        "event_id": "unique_event_id_1",
        "timestamp": "2023-10-01T12:00:00Z",
        "source": "test_source",
        "payload": {"data": "test_data"}
    }

def test_publish_event(sample_event):
    response = client.post("/publish", json=sample_event)
    assert response.status_code == 200
    assert response.json() == {"message": "Event published successfully"}

def test_publish_duplicate_event(sample_event):
    client.post("/publish", json=sample_event)  # Publish first time
    response = client.post("/publish", json=sample_event)  # Publish duplicate
    assert response.status_code == 200
    assert response.json() == {"message": "Event published successfully"}

def test_get_events():
    response = client.get("/events?topic=test_topic")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_stats():
    response = client.get("/stats")
    assert response.status_code == 200
    stats = response.json()
    assert "received" in stats
    assert "unique_processed" in stats
    assert "duplicate_dropped" in stats
    assert "topics" in stats
    assert "uptime" in stats

def test_invalid_event_schema():
    invalid_event = {
        "topic": "test_topic",
        "event_id": "unique_event_id_2",
        "timestamp": "invalid_timestamp",
        "source": "test_source",
        "payload": {}
    }
    response = client.post("/publish", json=invalid_event)
    assert response.status_code == 422  # Unprocessable Entity for invalid schema