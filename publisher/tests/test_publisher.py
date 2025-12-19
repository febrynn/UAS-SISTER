import pytest
from publisher.publisher import Publisher

@pytest.fixture
def publisher():
    return Publisher()

def test_publish_single_event(publisher):
    event = {
        "topic": "test_topic",
        "event_id": "unique_event_id_1",
        "timestamp": "2023-10-01T12:00:00Z",
        "source": "test_source",
        "payload": {"data": "test_data"}
    }
    response = publisher.publish(event)
    assert response.status_code == 200
    assert response.json() == {"status": "success"}

def test_publish_duplicate_event(publisher):
    event = {
        "topic": "test_topic",
        "event_id": "unique_event_id_1",
        "timestamp": "2023-10-01T12:00:00Z",
        "source": "test_source",
        "payload": {"data": "test_data"}
    }
    publisher.publish(event)  # First publish
    response = publisher.publish(event)  # Duplicate publish
    assert response.status_code == 200
    assert response.json() == {"status": "duplicate"}

def test_publish_batch_events(publisher):
    events = [
        {
            "topic": "test_topic",
            "event_id": "unique_event_id_2",
            "timestamp": "2023-10-01T12:00:01Z",
            "source": "test_source",
            "payload": {"data": "test_data_2"}
        },
        {
            "topic": "test_topic",
            "event_id": "unique_event_id_3",
            "timestamp": "2023-10-01T12:00:02Z",
            "source": "test_source",
            "payload": {"data": "test_data_3"}
        }
    ]
    response = publisher.publish_batch(events)
    assert response.status_code == 200
    assert response.json() == {"status": "success", "count": 2}

def test_publish_batch_with_duplicate(publisher):
    events = [
        {
            "topic": "test_topic",
            "event_id": "unique_event_id_4",
            "timestamp": "2023-10-01T12:00:03Z",
            "source": "test_source",
            "payload": {"data": "test_data_4"}
        },
        {
            "topic": "test_topic",
            "event_id": "unique_event_id_4",  # Duplicate
            "timestamp": "2023-10-01T12:00:03Z",
            "source": "test_source",
            "payload": {"data": "test_data_4"}
        }
    ]
    response = publisher.publish_batch(events)
    assert response.status_code == 200
    assert response.json() == {"status": "success", "count": 1, "duplicates": 1}