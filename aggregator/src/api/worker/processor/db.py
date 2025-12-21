from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    DateTime,
    func,
    UniqueConstraint,
)
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
import os

# =====================================================
# DATABASE CONFIG
# =====================================================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///./test.db"  # 🔥 default aman untuk pytest
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"check_same_thread": False}
    if DATABASE_URL.startswith("sqlite")
    else {},
)

SessionLocal = scoped_session(
    sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )
)

Base = declarative_base()

# =====================================================
# MODEL
# =====================================================
class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    event_id = Column(String, nullable=False)
    timestamp = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("topic", "event_id", name="uq_topic_event_id"),
    )

# =====================================================
# SESSION HELPER
# =====================================================
@contextmanager
def session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# =====================================================
# DB INIT
# =====================================================
def init_db():
    Base.metadata.create_all(bind=engine)

# =====================================================
# CRUD
# =====================================================
def insert_event(topic: str, event_id: str):
    with session_scope() as session:
        try:
            event = ProcessedEvent(
                topic=topic,
                event_id=event_id,
            )
            session.add(event)
        except IntegrityError:
            # idempotent insert
            pass

def get_events(topic: str | None = None):
    with session_scope() as session:
        query = session.query(ProcessedEvent)
        if topic:
            query = query.filter(ProcessedEvent.topic == topic)

        return [
            {
                "id": e.id,
                "topic": e.topic,
                "event_id": e.event_id,
                "timestamp": e.timestamp,
            }
            for e in query.all()
        ]

def get_stats():
    with session_scope() as session:
        return (
            session.query(
                ProcessedEvent.topic,
                func.count(ProcessedEvent.id),
            )
            .group_by(ProcessedEvent.topic)
            .all()
        )
