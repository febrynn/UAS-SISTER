import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import IntegrityError

from src.api.worker.processor import db
from src.api.worker.processor.db import ProcessedEvent

def setup_module():
    db.engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )

    db.SessionLocal = scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=db.engine,
        )
    )

    db.Base.metadata.bind = db.engine
    db.init_db()

# ✅ TAMBAHKAN FIXTURE db_session
@pytest.fixture
def db_session():
    """Create a database session for testing"""
    session = db.SessionLocal()
    yield session
    session.close()

def test_insert_event():
    db.insert_event("db_test", "e1")
    events = db.get_events("db_test")
    assert len(events) == 1

def test_insert_duplicate_event(db_session):
    # 1. Insert Pertama (Harus Sukses)
    event1 = ProcessedEvent(topic="t1", event_id="e1")
    db_session.add(event1)
    db_session.commit()

    # 2. Insert Kedua (Harus Error IntegrityError)
    event2 = ProcessedEvent(topic="t1", event_id="e1")
    db_session.add(event2)
    
    # 🔥 PERBAIKAN: Gunakan pytest.raises untuk menangkap error
    with pytest.raises(IntegrityError):
        db_session.commit()
    
    # Rollback agar session bersih kembali untuk test lain
    db_session.rollback()

def test_get_events_by_topic():
    db.insert_event("topic_a", "1")
    db.insert_event("topic_b", "2")

    events_a = db.get_events("topic_a")
    assert all(e["topic"] == "topic_a" for e in events_a)

def test_get_stats():
    db.insert_event("stat_test", "s1")
    stats = db.get_stats()
    assert isinstance(stats, list)

    if stats:
        topic, count = stats[0]
        assert isinstance(topic, str)
        assert isinstance(count, int)

# ✅ TEST BARU 1: Test insert multiple events berbeda
def test_insert_multiple_unique_events():
    """Test inserting multiple events with different event_ids"""
    topic = "multi_test"
    
    # Insert 3 events berbeda
    db.insert_event(topic, "event_1")
    db.insert_event(topic, "event_2")
    db.insert_event(topic, "event_3")
    
    # Verify semua berhasil disimpan
    events = db.get_events(topic)
    assert len(events) == 3
    
    # Verify event_ids berbeda
    event_ids = [e["event_id"] for e in events]
    assert "event_1" in event_ids
    assert "event_2" in event_ids
    assert "event_3" in event_ids

# ✅ TEST BARU 2: Test get_events returns empty list for non-existent topic
def test_get_events_nonexistent_topic():
    """Test that get_events returns empty list for topic that doesn't exist"""
    events = db.get_events("nonexistent_topic_12345")
    
    # Verify returns list (not None or error)
    assert isinstance(events, list)
    
    # Verify list is empty
    assert len(events) == 0