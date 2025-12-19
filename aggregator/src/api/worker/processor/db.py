from sqlalchemy import create_engine, Column, String, Integer, DateTime, func, UniqueConstraint
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:Febriani_20@uas-postgres:5432/db")

Base = declarative_base()
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = scoped_session(sessionmaker(bind=engine))

class ProcessedEvent(Base):
    __tablename__ = 'processed_events'
    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    event_id = Column(String, nullable=False)
    timestamp = Column(DateTime, default=func.now())
    
    __table_args__ = (
        UniqueConstraint('topic', 'event_id', name='uq_topic_event_id'),
    )

@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def init_db():
    Base.metadata.create_all(engine)

def insert_event(topic, event_id):
    with session_scope() as session:
        try:
            event = ProcessedEvent(topic=topic, event_id=event_id)
            session.add(event)
        except IntegrityError:
            pass # Idempotensi

def get_events(topic=None):
    with session_scope() as session:
        query = session.query(ProcessedEvent)
        if topic:
            query = query.filter(ProcessedEvent.topic == topic)
        # Mengembalikan data sebagai list murni agar tidak error saat session tutup
        return [
            {"id": e.id, "topic": e.topic, "event_id": e.event_id, "timestamp": e.timestamp} 
            for e in query.all()
        ]

def get_stats():
    with session_scope() as session:
        return session.query(
            ProcessedEvent.topic, 
            func.count(ProcessedEvent.id)
        ).group_by(ProcessedEvent.topic).all()