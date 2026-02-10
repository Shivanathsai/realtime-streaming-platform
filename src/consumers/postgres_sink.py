"""PostgreSQL sink — consumes enriched events and persists to database with batched writes."""

import time
from datetime import datetime
from typing import List

from config.settings import get_config
from src.models.events import RawEvent, Alert, WindowAggregate
from src.consumers.database import DatabaseManager, EventRecord, AlertRecord, WindowAggregateRecord
from src.utils.kafka_client import EventConsumer
from src.utils.logging import setup_logging, get_logger
from src.utils import metrics as m

logger = get_logger(__name__)


class PostgresSink:
    """Batched PostgreSQL writer consuming from Kafka topics."""

    def __init__(self, batch_size: int = 500, flush_interval: float = 5.0):
        self.config = get_config()
        self.db = DatabaseManager(self.config.postgres)
        self.db.create_tables()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._event_buffer: List[dict] = []
        self._alert_buffer: List[dict] = []
        self._window_buffer: List[dict] = []
        self._last_flush = time.time()
        self._total_written = 0
        logger.info("postgres_sink_initialized", batch_size=batch_size)

    def handle_enriched_event(self, msg):
        try:
            raw = RawEvent.deserialize(msg.value())
            record = {
                "event_id": raw.event_id,
                "event_type": raw.event_type.value,
                "user_id": raw.user_id,
                "timestamp": raw.timestamp,
                "amount": raw.properties.get("amount"),
                "merchant_id": raw.properties.get("merchant_id"),
                "merchant_category": raw.properties.get("merchant_category"),
                "payment_method": raw.properties.get("payment_method"),
                "location_lat": raw.properties.get("location_lat"),
                "location_lon": raw.properties.get("location_lon"),
                "risk_score": raw.properties.get("risk_score"),
                "properties": raw.properties,
                "processed_at": datetime.utcnow(),
            }
            self._event_buffer.append(record)
            self._maybe_flush()
        except Exception as e:
            logger.error("event_parse_error", error=str(e))
            m.deadletter_events.labels(reason="parse_error").inc()

    def handle_alert(self, msg):
        try:
            alert = Alert.deserialize(msg.value())
            record = {
                "alert_id": alert.alert_id,
                "timestamp": alert.timestamp,
                "severity": alert.severity.value,
                "pattern_name": alert.pattern_name,
                "user_id": alert.user_id,
                "description": alert.description,
                "trigger_events": alert.trigger_events,
                "context": alert.context,
            }
            self._alert_buffer.append(record)
            self._maybe_flush()
        except Exception as e:
            logger.error("alert_parse_error", error=str(e))

    def handle_window_aggregate(self, msg):
        try:
            data = WindowAggregate.model_validate_json(msg.value())
            record = {
                "window_start": data.window_start,
                "window_end": data.window_end,
                "window_type": data.window_type,
                "group_key": data.group_key,
                "event_count": data.event_count,
                "sum_amount": data.sum_amount,
                "avg_amount": data.avg_amount,
                "min_amount": data.min_amount,
                "max_amount": data.max_amount,
                "distinct_merchants": data.distinct_merchants,
            }
            self._window_buffer.append(record)
            self._maybe_flush()
        except Exception as e:
            logger.error("window_parse_error", error=str(e))

    def _maybe_flush(self):
        total = len(self._event_buffer) + len(self._alert_buffer) + len(self._window_buffer)
        elapsed = time.time() - self._last_flush
        if total >= self.batch_size or elapsed >= self.flush_interval:
            self._flush()

    def _flush(self):
        session = self.db.get_session()
        try:
            if self._event_buffer:
                session.bulk_insert_mappings(EventRecord, self._event_buffer)
                written = len(self._event_buffer)
                self._event_buffer.clear()
                self._total_written += written

            if self._alert_buffer:
                session.bulk_insert_mappings(AlertRecord, self._alert_buffer)
                self._alert_buffer.clear()

            if self._window_buffer:
                session.bulk_insert_mappings(WindowAggregateRecord, self._window_buffer)
                self._window_buffer.clear()

            session.commit()
            self._last_flush = time.time()
            logger.debug("batch_flushed", total_written=self._total_written)

        except Exception as e:
            session.rollback()
            logger.error("flush_failed", error=str(e))
        finally:
            session.close()


def run_event_sink():
    """Consume enriched events and write to PostgreSQL."""
    setup_logging()
    config = get_config()
    sink = PostgresSink(batch_size=500, flush_interval=5.0)

    consumer = EventConsumer(
        config.kafka,
        group_id="postgres-event-sink",
        topics=[config.kafka.enriched_events_topic],
    )
    logger.info("event_sink_started")
    consumer.consume_loop(handler=sink.handle_enriched_event, commit_interval=500)


def run_alert_sink():
    """Consume alerts and write to PostgreSQL."""
    setup_logging()
    config = get_config()
    sink = PostgresSink(batch_size=50, flush_interval=2.0)

    consumer = EventConsumer(
        config.kafka,
        group_id="postgres-alert-sink",
        topics=[config.kafka.alerts_topic],
    )
    logger.info("alert_sink_started")
    consumer.consume_loop(handler=sink.handle_alert, commit_interval=50)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "alerts":
        run_alert_sink()
    else:
        run_event_sink()
