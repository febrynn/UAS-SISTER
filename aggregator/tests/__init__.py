import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

import src.api.worker.processor.db as db
from src.main import app

# =====================================================
# FIXTURE
# =====================================================
@pytest.fixture(scope="function")
def client():
    # 🔥 Buat engine baru untuk setiap test
    test_engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    
    TestingSessionLocal = scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=test_engine,
        )
    )

    # 🔥 Override GLOBAL DB objects
    original_engine = db.engine
    original_session = db.SessionLocal
    
    db.engine = test_engine
    db.SessionLocal = TestingSessionLocal
    db.Base.metadata.bind = test_engine

    # 🔥 INIT TABLE
    db.Base.metadata.create_all(bind=test_engine)

    with TestClient(app) as c:
        yield c

    # 🔥 CLEANUP
    db.Base.metadata.drop_all(bind=test_engine)
    TestingSessionLocal.remove()
    
    # Restore original (opsional)
    db.engine = original_engine
    db.SessionLocal = original_session