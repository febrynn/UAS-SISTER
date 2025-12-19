from sqlalchemy import create_engine, Column, String, Integer, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@storage:5432/db")

Base = declarative_base()
engine = create_engine(DATABASE_URL)
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

def init_db():
    Base.metadata.create_all(engine)

def upsert_event(topic, event_id):
    session = Session()
    try:
        event = ProcessedEvent(topic=topic, event_id=event_id)
        session.add(event)
        session.commit()
    except Exception as e:
        session.rollback()
        if "unique constraint" in str(e):
            pass  # Ignore duplicate entry
    finally:
        session.close()