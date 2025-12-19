import pytest
import requests

BASE_URL = "http://localhost:8080"

def test_stats_consistency_after_events():
    # Publish a batch of events
    events = [
        {"topic": "test_topic", "event_id": "1", "timestamp": "2023-10-01T12:00:00Z", "source": "test_source", "payload": {"data": "value1"}},
        {"topic": "test_topic", "event_id": "2", "timestamp": "2023-10-01T12:00:01Z", "source": "test_source", "payload": {"data": "value2"}},
        {"topic": "test_topic", "event_id": "1", "timestamp": "2023-10-01T12:00:02Z", "source": "test_source", "payload": {"data": "value1"}},  # Duplicate
    ]
    
    response = requests.post(f"{BASE_URL}/publish", json=events)
    assert response.status_code == 200

    # Check stats
    response = requests.get(f"{BASE_URL}/stats")
    assert response.status_code == 200
    stats = response.json()

    assert stats['received'] == 3
    assert stats['unique_processed'] == 2
    assert stats['duplicate_dropped'] == 1
    assert stats['topics'] == 1

def test_stats_consistency_after_reprocessing():
    # Reprocess the same events
    events = [
        {"topic": "test_topic", "event_id": "1", "timestamp": "2023-10-01T12:00:00Z", "source": "test_source", "payload": {"data": "value1"}},
        {"topic": "test_topic", "event_id": "2", "timestamp": "2023-10-01T12:00:01Z", "source": "test_source", "payload": {"data": "value2"}},
    ]
    
    response = requests.post(f"{BASE_URL}/publish", json=events)
    assert response.status_code == 200

    # Check stats again
    response = requests.get(f"{BASE_URL}/stats")
    assert response.status_code == 200
    stats = response.json()

    assert stats['received'] == 5  # 3 from first + 2 from reprocessing
    assert stats['unique_processed'] == 2  # Still only 2 unique events
    assert stats['duplicate_dropped'] == 3  # 1 from first + 2 from reprocessing
    assert stats['topics'] == 1