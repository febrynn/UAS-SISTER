import pytest
import requests

BASE_URL = "http://localhost:8080"

def test_transactional_batch():
    # Prepare a batch of events
    events = [
        {"topic": "test_topic", "event_id": "1", "timestamp": "2023-10-01T12:00:00Z", "source": "test_source", "payload": {"data": "value1"}},
        {"topic": "test_topic", "event_id": "2", "timestamp": "2023-10-01T12:00:01Z", "source": "test_source", "payload": {"data": "value2"}},
        {"topic": "test_topic", "event_id": "1", "timestamp": "2023-10-01T12:00:02Z", "source": "test_source", "payload": {"data": "value1"}},  # Duplicate
    ]

    # Send batch of events
    response = requests.post(f"{BASE_URL}/publish", json=events)
    assert response.status_code == 200

    # Check the processed events
    response = requests.get(f"{BASE_URL}/events?topic=test_topic")
    assert response.status_code == 200
    processed_events = response.json()
    
    # Validate that only unique events are processed
    assert len(processed_events) == 2  # Only unique events should be present
    assert any(event['event_id'] == "1" for event in processed_events)
    assert any(event['event_id'] == "2" for event in processed_events)

    # Check stats
    response = requests.get(f"{BASE_URL}/stats")
    assert response.status_code == 200
    stats = response.json()
    
    # Validate stats
    assert stats['received'] == 3  # Total events received
    assert stats['unique_processed'] == 2  # Unique events processed
    assert stats['duplicate_dropped'] == 1  # Duplicate dropped
    assert stats['topics'] == ["test_topic"]  # Check topic
    assert stats['uptime'] is not None  # Uptime should be present