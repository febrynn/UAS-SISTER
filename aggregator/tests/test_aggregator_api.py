from datetime import datetime

def valid_event(event_id="evt1", topic="test_topic"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test-source",
        "payload": {
            "data": {"value": 123}
        }
    }


def test_healthcheck(client):
    res = client.get("/")
    assert res.status_code == 200
    assert res.json()["status"] == "healthy"


def test_publish_single_event(client):
    res = client.post("/publish", json=[valid_event()])
    assert res.status_code == 200
    assert res.json()["message"] == "Events processed"


def test_publish_batch_events(client):
    events = [
        valid_event("evt2"),
        valid_event("evt3")
    ]
    res = client.post("/publish", json=events)
    assert res.status_code == 200


def test_publish_duplicate_event(client):
    event = valid_event("dup1")
    client.post("/publish", json=[event])
    res = client.post("/publish", json=[event])
    assert res.status_code == 200


def test_publish_batch_with_duplicate(client):
    events = [
        valid_event("dup2"),
        valid_event("dup2"),
    ]
    res = client.post("/publish", json=events)
    assert res.status_code == 200




def test_invalid_schema_timestamp(client):
    invalid_event = {
        "topic": "bad",
        "event_id": "bad1",
        "timestamp": "not-a-date",
        "source": "test",
        "payload": {"data": {}}
    }
    res = client.post("/publish", json=[invalid_event])
    assert res.status_code == 422
