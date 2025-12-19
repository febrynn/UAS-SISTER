import pytest
import asyncio
from httpx import AsyncClient
from aggregator.src.main import app

@pytest.mark.asyncio
async def test_concurrent_event_processing():
    async def publish_event(event_data):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post("/publish", json=event_data)
            return response

    event_data = {
        "topic": "test_topic",
        "event_id": "unique_event_id",
        "timestamp": "2023-10-01T12:00:00Z",
        "source": "test_source",
        "payload": {"key": "value"}
    }

    # Create a list of tasks for concurrent publishing
    tasks = [publish_event(event_data) for _ in range(10)]
    
    # Run tasks concurrently
    responses = await asyncio.gather(*tasks)

    # Check that all responses are successful
    for response in responses:
        assert response.status_code == 200

    # Check that the event was processed only once
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/events?topic=test_topic")
        events = response.json()
        assert len(events) == 1  # Ensure only one unique event is processed

    # Check stats for duplicates dropped
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/stats")
        stats = response.json()
        assert stats['duplicate_dropped'] == 9  # 9 duplicates should be dropped
        assert stats['unique_processed'] == 1  # Only one unique event should be processed