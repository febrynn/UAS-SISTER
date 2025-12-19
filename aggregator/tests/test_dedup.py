import pytest
from fastapi.testclient import TestClient
from src.main import app
from src.db import get_db
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from src.models import Base, ProcessedEvent

@pytest.fixture(scope="module")
def test_client():
    engine = create_engine("postgresql://user:pass@localhost/test_db")
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Create the database tables
    Base.metadata.create_all(bind=engine)
    
    yield TestClient(app)

    # Drop the database tables after tests
    Base.metadata.drop_all(bind=engine)

@pytest.fixture
def db_session(test_client):
    db = get_db()
    yield db
    db.close()

def test_deduplication_single_event(db_session):
    event = {
        "topic": "test_topic",
        "event_id": "unique_event_1",
        "timestamp": "2023-10-01T12:00:00Z",
        "source": "test_source",
        "payload": {"data": "test_data"}
    }
    
    response = test_client.post("/publish", json=event)
    assert response.status_code == 200

    # Check if the event is stored
    stored_event = db_session.query(ProcessedEvent).filter_by(topic=event["topic"], event_id=event["event_id"]).first()
    assert stored_event is not None

    # Publish the same event again
    response = test_client.post("/publish", json=event)
    assert response.status_code == 200

    # Ensure no duplicate entry is created
    duplicate_count = db_session.query(ProcessedEvent).filter_by(topic=event["topic"], event_id=event["event_id"]).count()
    assert duplicate_count == 1

def test_deduplication_batch_events(db_session):
    events = [
        {
            "topic": "test_topic",
            "event_id": "unique_event_2",
            "timestamp": "2023-10-01T12:00:00Z",
            "source": "test_source",
            "payload": {"data": "test_data_1"}
        },
        {
            "topic": "test_topic",
            "event_id": "unique_event_2",  # Duplicate event
            "timestamp": "2023-10-01T12:00:00Z",
            "source": "test_source",
            "payload": {"data": "test_data_1"}
        }
    ]
    
    response = test_client.post("/publish", json=events)
    assert response.status_code == 200

    # Ensure only one event is stored
    unique_count = db_session.query(ProcessedEvent).filter_by(topic="test_topic", event_id="unique_event_2").count()
    assert unique_count == 1

def test_event_schema_validation():
    invalid_event = {
        "topic": "test_topic",
        "event_id": "unique_event_3",
        "timestamp": "invalid_timestamp",  # Invalid timestamp
        "source": "test_source",
        "payload": {"data": "test_data"}
    }
    
    response = test_client.post("/publish", json=invalid_event)
    assert response.status_code == 422  # Unprocessable Entity for validation error