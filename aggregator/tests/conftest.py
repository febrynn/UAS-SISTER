import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

import src.api.worker.processor.db as db
from src.main import app

# =====================================================
# TEST DATABASE (IN-MEMORY SQLITE)
# =====================================================
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
)

TestingSessionLocal = scoped_session(
    sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )
)

# =====================================================
# FIXTURE
# =====================================================
@pytest.fixture(scope="function")
def client():
    # 🔥 Override GLOBAL DB objects
    db.engine = engine
    db.SessionLocal = TestingSessionLocal
    db.Base.metadata.bind = engine

    # 🔥 INIT TABLE - Pastikan table dibuat setiap kali
    db.Base.metadata.create_all(bind=engine)
    
    # Atau gunakan:
    # db.init_db()

    with TestClient(app) as c:
        yield c

    # 🔥 CLEANUP - Drop all tables setelah test selesai
    db.Base.metadata.drop_all(bind=engine)
    TestingSessionLocal.remove()