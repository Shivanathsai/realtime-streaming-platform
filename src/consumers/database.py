"""PostgreSQL models and connection management."""

from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, Float, Integer, Boolean,
    DateTime, JSON, Index, BigInteger, Text, Enum as PgEnum,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB

from config.settings import PostgresConfig

Base = declarative_base()


class EventRecord(Base):
    __tablename__ = "events"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    event_id = Column(String(64), unique=True, nullable=False, index=True)
    event_type = Column(String(32), nullable=False)
    user_id = Column(String(32), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    amount = Column(Float)
    merchant_id = Column(String(32))
    merchant_category = Column(String(32))
    payment_method = Column(String(32))
    location_lat = Column(Float)
    location_lon = Column(Float)
    risk_score = Column(Float)
    properties = Column(JSONB)
    processed_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_events_user_ts", "user_id", "timestamp"),
        Index("idx_events_merchant", "merchant_id", "timestamp"),
    )


class AlertRecord(Base):
    __tablename__ = "alerts"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    alert_id = Column(String(64), unique=True, nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    severity = Column(String(16), nullable=False, index=True)
    pattern_name = Column(String(64), nullable=False, index=True)
    user_id = Column(String(32), nullable=False, index=True)
    description = Column(Text)
    trigger_events = Column(JSONB)
    context = Column(JSONB)
    acknowledged = Column(Boolean, default=False)
    acknowledged_at = Column(DateTime)

    __table_args__ = (
        Index("idx_alerts_user_sev", "user_id", "severity"),
        Index("idx_alerts_pattern", "pattern_name", "timestamp"),
    )


class WindowAggregateRecord(Base):
    __tablename__ = "window_aggregates"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    window_start = Column(DateTime, nullable=False)
    window_end = Column(DateTime, nullable=False)
    window_type = Column(String(16), nullable=False)
    group_key = Column(String(64), nullable=False)
    event_count = Column(Integer)
    sum_amount = Column(Float)
    avg_amount = Column(Float)
    min_amount = Column(Float)
    max_amount = Column(Float)
    distinct_merchants = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_window_key_ts", "group_key", "window_start"),
        Index("idx_window_type", "window_type", "window_start"),
    )


class DatabaseManager:
    def __init__(self, config: PostgresConfig):
        self.engine = create_engine(config.url, pool_size=10, max_overflow=20, pool_recycle=3600)
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def get_session(self):
        return self.Session()
