import os
from contextlib import contextmanager
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    DateTime,
    func,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
from sqlalchemy.exc import IntegrityError

# =====================================================
# DATABASE CONFIG
# =====================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:Febriani_20@storage:5432/db"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()


# =====================================================
# MODEL
# =====================================================

class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    event_id = Column(String, nullable=False)
    timestamp = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint("topic", "event_id", name="uq_topic_event_id"),
    )


# =====================================================
# DB HELPERS
# =====================================================

def init_db():
    """Create tables if not exists"""
    Base.metadata.create_all(engine)


@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def upsert_event(topic: str, event_id: str):
    """
    Idempotent insert:
    - event unik → masuk DB
    - event duplikat → diabaikan
    """
    with session_scope() as session:
        try:
            event = ProcessedEvent(topic=topic, event_id=event_id)
            session.add(event)
        except IntegrityError:
            # Duplicate → ignore
            pass
