"""Stream Processor — orchestrates windowing, CEP, enrichment, and state management."""

import time
from datetime import datetime

from config.settings import get_config
from src.models.events import RawEvent, Alert
from src.utils.kafka_client import EventConsumer, EventProducer
from src.utils.state_store import StateStore
from src.utils.logging import setup_logging, get_logger
from src.utils import metrics as m
from src.processors.windowing import TumblingWindow, SlidingWindow, SessionWindow
from src.processors.cep_engine import CEPEngine

logger = get_logger(__name__)


class StreamProcessor:
    """Core stream processor with stateful transformations.

    Pipeline: Raw Events → Deserialize → Enrich → Window → CEP → Route
    """

    def __init__(self):
        self.config = get_config()
        self.producer = EventProducer(self.config.kafka)

        # State stores (keyed by user_id)
        self.user_state = StateStore("user_state", ttl_seconds=self.config.processor.state_ttl_seconds)
        self.window_state = StateStore("window_state", ttl_seconds=3600)

        # Windows
        self.tumbling = TumblingWindow(self.config.processor.tumbling_window_seconds)
        self.sliding = SlidingWindow(
            self.config.processor.sliding_window_seconds,
            self.config.processor.sliding_window_step_seconds,
        )
        self.session = SessionWindow(self.config.processor.session_window_gap_seconds)

        # CEP engine
        self.cep = CEPEngine(self.user_state, config={
            "velocity_threshold": self.config.processor.velocity_threshold,
            "amount_spike_threshold": self.config.processor.amount_spike_threshold,
            "geo_velocity_kmh": self.config.processor.geo_velocity_kmh,
            "rapid_fire_count": self.config.processor.rapid_fire_count,
            "rapid_fire_window_seconds": self.config.processor.rapid_fire_window_seconds,
        })

        self._processed = 0
        self._start_time = time.time()
        logger.info("stream_processor_initialized")

    def process_message(self, msg):
        """Process a single Kafka message through the full pipeline."""
        start = time.time()
        try:
            raw = RawEvent.deserialize(msg.value())
            event_data = {"event_id": raw.event_id, "timestamp": raw.timestamp.isoformat(),
                          "user_id": raw.user_id, **raw.properties}

            # Step 1: Enrich with state
            enriched = self._enrich(raw.user_id, event_data)

            # Step 2: Windowed aggregations
            self._process_windows(raw.user_id, enriched, raw.timestamp)

            # Step 3: CEP pattern detection
            alerts = self.cep.evaluate(raw.user_id, enriched)
            for alert in alerts:
                self._route_alert(alert)

            # Step 4: Forward enriched event
            self.producer.produce(
                topic=self.config.kafka.enriched_events_topic,
                key=raw.user_id,
                value=RawEvent(
                    event_id=raw.event_id, event_type=raw.event_type,
                    timestamp=raw.timestamp, user_id=raw.user_id,
                    properties=enriched, metadata={"processed_at": datetime.utcnow().isoformat()},
                ).serialize(),
            )

            self._processed += 1
            m.events_processed.labels(processor="stream").inc()
            m.processing_latency.labels(processor="stream").observe(time.time() - start)

            if self._processed % 10000 == 0:
                elapsed = time.time() - self._start_time
                eps = self._processed / max(elapsed, 0.001)
                m.pipeline_throughput.set(eps)
                m.state_size.labels(processor="user_state").set(self.user_state.size())
                logger.info("processor_throughput",
                            total=self._processed, eps=round(eps, 1),
                            state_keys=self.user_state.size())

        except Exception as e:
            logger.error("processing_error", error=str(e), topic=msg.topic(),
                         partition=msg.partition(), offset=msg.offset())
            self._route_deadletter(msg, str(e))
            m.deadletter_events.labels(reason="processing_error").inc()

    def _enrich(self, user_id: str, event: dict) -> dict:
        """Enrich event with user state and computed features."""
        state = self.user_state.get(user_id) or {}
        event["user_avg_amount"] = state.get("running_mean", 0)
        event["user_txn_count_1h"] = len(state.get("events", []))
        event["user_event_count_total"] = state.get("count", 0)
        return event

    def _process_windows(self, user_id: str, event: dict, ts: datetime):
        """Feed event into all window types and forward results."""
        for window in [self.tumbling, self.sliding, self.session]:
            result = window.add(user_id, event, ts)
            if result:
                self.producer.produce(
                    topic=self.config.kafka.metrics_topic,
                    key=user_id,
                    value=result.serialize(),
                    headers={"window_type": result.window_type},
                )

    def _route_alert(self, alert: Alert):
        self.producer.produce(
            topic=self.config.kafka.alerts_topic,
            key=alert.user_id,
            value=alert.serialize(),
            headers={"severity": alert.severity.value, "pattern": alert.pattern_name},
        )
        logger.warning("alert_generated", severity=alert.severity.value,
                        pattern=alert.pattern_name, user=alert.user_id,
                        description=alert.description)

    def _route_deadletter(self, msg, error: str):
        try:
            self.producer.produce(
                topic=self.config.kafka.deadletter_topic,
                key=msg.key() or b"unknown",
                value=msg.value(),
                headers={"error": error, "original_topic": msg.topic()},
            )
        except Exception:
            logger.error("deadletter_routing_failed")


def run_processor():
    """Main entry point for the stream processor."""
    setup_logging()
    config = get_config()

    processor = StreamProcessor()
    consumer = EventConsumer(
        config.kafka,
        group_id="stream-processor",
        topics=[config.kafka.raw_events_topic],
    )

    logger.info("stream_processor_started",
                topics=[config.kafka.raw_events_topic])

    consumer.consume_loop(
        handler=processor.process_message,
        commit_interval=100,
    )


if __name__ == "__main__":
    run_processor()
