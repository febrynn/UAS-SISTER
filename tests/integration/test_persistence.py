import pytest
from httpx import AsyncClient
from aggregator.src.main import app

@pytest.mark.asyncio
async def test_event_persistence():
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Test publishing a single event
        response = await client.post("/publish", json={
            "topic": "test_topic",
            "event_id": "unique_event_1",
            "timestamp": "2023-10-01T12:00:00Z",
            "source": "test_source",
            "payload": {"data": "test_data"}
        })
        assert response.status_code == 200

        # Test retrieving the published event
        response = await client.get("/events?topic=test_topic")
        assert response.status_code == 200
        events = response.json()
        assert len(events) == 1
        assert events[0]["event_id"] == "unique_event_1"

        # Test publishing a duplicate event
        response = await client.post("/publish", json={
            "topic": "test_topic",
            "event_id": "unique_event_1",
            "timestamp": "2023-10-01T12:00:00Z",
            "source": "test_source",
            "payload": {"data": "test_data"}
        })
        assert response.status_code == 200

        # Check stats for duplicates
        response = await client.get("/stats")
        assert response.status_code == 200
        stats = response.json()
        assert stats["duplicate_dropped"] == 1

        # Test publishing another unique event
        response = await client.post("/publish", json={
            "topic": "test_topic",
            "event_id": "unique_event_2",
            "timestamp": "2023-10-01T12:01:00Z",
            "source": "test_source",
            "payload": {"data": "test_data_2"}
        })
        assert response.status_code == 200

        # Verify both unique events are stored
        response = await client.get("/events?topic=test_topic")
        assert response.status_code == 200
        events = response.json()
        assert len(events) == 2
        assert {event["event_id"] for event in events} == {"unique_event_1", "unique_event_2"}