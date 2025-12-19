import pytest
import asyncio
import requests
from httpx import AsyncClient
from aggregator.src.main import app

BASE_URL = "http://localhost:8080"

# =========================================================
# ASYNC / UNIT-STYLE TESTS (FastAPI app langsung)
# =========================================================

@pytest.mark.asyncio
async def test_single_event_publish():
    async with AsyncClient(app=app, base_url="http://test") as client:
        res = await client.post("/publish", json={
            "topic": "unit_topic",
            "event_id": "evt1",
            "timestamp": "2023-10-01T10:00:00Z",
            "source": "unit_test",
            "payload": {"a": 1}
        })
        assert res.status_code == 200


@pytest.mark.asyncio
async def test_duplicate_event_dropped():
    async with AsyncClient(app=app, base_url="http://test") as client:
        event = {
            "topic": "unit_topic",
            "event_id": "dup_evt",
            "timestamp": "2023-10-01T10:01:00Z",
            "source": "unit_test",
            "payload": {"a": 2}
        }

        await client.post("/publish", json=event)
        await client.post("/publish", json=event)

        stats = (await client.get("/stats")).json()
        assert stats["duplicate_dropped"] >= 1


@pytest.mark.asyncio
async def test_event_persistence():
    async with AsyncClient(app=app, base_url="http://test") as client:
        await client.post("/publish", json={
            "topic": "persist_topic",
            "event_id": "persist1",
            "timestamp": "2023-10-01T10:02:00Z",
            "source": "unit_test",
            "payload": {}
        })

        res = await client.get("/events?topic=persist_topic")
        events = res.json()
        assert len(events) == 1


@pytest.mark.asyncio
async def test_concurrent_event_processing():
    async def send():
        async with AsyncClient(app=app, base_url="http://test") as client:
            return await client.post("/publish", json={
                "topic": "concurrent_topic",
                "event_id": "same_evt",
                "timestamp": "2023-10-01T10:03:00Z",
                "source": "unit_test",
                "payload": {}
            })

    responses = await asyncio.gather(*[send() for _ in range(10)])
    assert all(r.status_code == 200 for r in responses)

    async with AsyncClient(app=app, base_url="http://test") as client:
        events = (await client.get("/events?topic=concurrent_topic")).json()
        stats = (await client.get("/stats")).json()

    assert len(events) == 1
    assert stats["duplicate_dropped"] >= 9


@pytest.mark.asyncio
async def test_multiple_topics():
    async with AsyncClient(app=app, base_url="http://test") as client:
        await client.post("/publish", json={
            "topic": "topic_a",
            "event_id": "a1",
            "timestamp": "2023-10-01T10:04:00Z",
            "source": "unit_test",
            "payload": {}
        })
        await client.post("/publish", json={
            "topic": "topic_b",
            "event_id": "b1",
            "timestamp": "2023-10-01T10:05:00Z",
            "source": "unit_test",
            "payload": {}
        })

        stats = (await client.get("/stats")).json()
        assert stats["topics"] >= 2


# =========================================================
# INTEGRATION TESTS (Docker / HTTP REAL)
# =========================================================

def test_publish_batch():
    events = [
        {"topic": "int_topic", "event_id": "1", "timestamp": "2023-10-01T12:00:00Z", "source": "it", "payload": {}},
        {"topic": "int_topic", "event_id": "2", "timestamp": "2023-10-01T12:00:01Z", "source": "it", "payload": {}},
    ]
    res = requests.post(f"{BASE_URL}/publish", json=events)
    assert res.status_code == 200


def test_transactional_batch():
    events = [
        {"topic": "txn_topic", "event_id": "1", "timestamp": "2023-10-01T12:10:00Z", "source": "it", "payload": {}},
        {"topic": "txn_topic", "event_id": "2", "timestamp": "2023-10-01T12:10:01Z", "source": "it", "payload": {}},
        {"topic": "txn_topic", "event_id": "1", "timestamp": "2023-10-01T12:10:02Z", "source": "it", "payload": {}},
    ]

    requests.post(f"{BASE_URL}/publish", json=events)

    ev = requests.get(f"{BASE_URL}/events?topic=txn_topic").json()
    stats = requests.get(f"{BASE_URL}/stats").json()

    assert len(ev) == 2
    assert stats["duplicate_dropped"] >= 1


def test_stats_consistency_after_events():
    stats = requests.get(f"{BASE_URL}/stats").json()
    assert stats["received"] >= stats["unique_processed"]
    assert stats["uptime"] is not None


def test_stats_topics_count():
    stats = requests.get(f"{BASE_URL}/stats").json()
    assert stats["topics"] >= 1


def test_republish_existing_events():
    event = {
        "topic": "republish_topic",
        "event_id": "x1",
        "timestamp": "2023-10-01T12:20:00Z",
        "source": "it",
        "payload": {}
    }

    requests.post(f"{BASE_URL}/publish", json=event)
    requests.post(f"{BASE_URL}/publish", json=event)

    stats = requests.get(f"{BASE_URL}/stats").json()
    assert stats["duplicate_dropped"] >= 1


def test_events_endpoint_filter():
    res = requests.get(f"{BASE_URL}/events?topic=txn_topic")
    assert res.status_code == 200


def test_empty_topic_events():
    res = requests.get(f"{BASE_URL}/events?topic=nonexistent")
    assert res.status_code == 200
    assert res.json() == []


def test_service_health():
    res = requests.get(f"{BASE_URL}/stats")
    assert res.status_code == 200


def test_system_uptime_exists():
    stats = requests.get(f"{BASE_URL}/stats").json()
    assert "uptime" in stats
