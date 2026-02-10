"""Kafka producer and consumer wrappers with exactly-once semantics."""

import time
from typing import Optional, Callable
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from config.settings import KafkaConfig
from src.utils.logging import get_logger
from src.utils import metrics as m

logger = get_logger(__name__)


class EventProducer:
    """Kafka producer with idempotent writes and delivery guarantees."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self._producer = Producer({
            "bootstrap.servers": config.bootstrap_servers,
            "acks": config.acks,
            "retries": config.retries,
            "linger.ms": config.linger_ms,
            "batch.size": config.batch_size,
            "compression.type": config.compression_type,
            "enable.idempotence": config.idempotent,
            "max.in.flight.requests.per.connection": 5,
        })
        logger.info("producer_initialized", servers=config.bootstrap_servers)

    def produce(self, topic: str, key: str, value: bytes,
                headers: Optional[dict] = None, on_delivery: Optional[Callable] = None):
        start = time.time()
        try:
            kafka_headers = [(k, v.encode() if isinstance(v, str) else v)
                             for k, v in (headers or {}).items()]
            self._producer.produce(
                topic=topic,
                key=key.encode() if isinstance(key, str) else key,
                value=value,
                headers=kafka_headers,
                callback=on_delivery or self._default_callback,
            )
            self._producer.poll(0)
            m.events_produced.labels(topic=topic, event_type="raw").inc()
            m.produce_latency.labels(topic=topic).observe(time.time() - start)
        except KafkaException as e:
            m.produce_errors.labels(topic=topic).inc()
            logger.error("produce_failed", topic=topic, key=key, error=str(e))
            raise

    def flush(self, timeout: float = 10.0):
        self._producer.flush(timeout)

    def _default_callback(self, err, msg):
        if err:
            logger.error("delivery_failed", topic=msg.topic(), error=str(err))
            m.produce_errors.labels(topic=msg.topic()).inc()

    def close(self):
        self._producer.flush(30)
        logger.info("producer_closed")


class EventConsumer:
    """Kafka consumer with manual offset commit for exactly-once processing."""

    def __init__(self, config: KafkaConfig, group_id: str = None, topics: list[str] = None):
        self.config = config
        self.group_id = group_id or config.consumer_group
        self._consumer = Consumer({
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": config.auto_offset_reset,
            "enable.auto.commit": False,  # manual commit for exactly-once
            "max.poll.interval.ms": config.max_poll_interval_ms,
            "session.timeout.ms": config.session_timeout_ms,
            "isolation.level": "read_committed",  # transactional reads
        })
        if topics:
            self._consumer.subscribe(topics, on_assign=self._on_assign)
        self._running = False
        logger.info("consumer_initialized", group=self.group_id, topics=topics)

    def consume_loop(self, handler: Callable, poll_timeout: float = 1.0,
                     batch_size: int = 100, commit_interval: int = 100):
        """Main consume loop with batched processing and manual commit."""
        self._running = True
        msg_count = 0
        logger.info("consume_loop_started", group=self.group_id)

        try:
            while self._running:
                msg = self._consumer.poll(poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("consume_error", error=msg.error().str())
                    m.consume_errors.labels(topic=msg.topic()).inc()
                    continue

                try:
                    handler(msg)
                    msg_count += 1
                    m.events_consumed.labels(
                        topic=msg.topic(), consumer_group=self.group_id
                    ).inc()

                    if msg_count % commit_interval == 0:
                        self._consumer.commit(asynchronous=False)
                        logger.debug("offsets_committed", count=msg_count)

                except Exception as e:
                    logger.error("handler_error", error=str(e),
                                 topic=msg.topic(), partition=msg.partition(),
                                 offset=msg.offset())
                    m.consume_errors.labels(topic=msg.topic()).inc()

        except KeyboardInterrupt:
            logger.info("consume_loop_interrupted")
        finally:
            self._consumer.commit(asynchronous=False)
            self._consumer.close()
            logger.info("consumer_closed", total_processed=msg_count)

    def stop(self):
        self._running = False

    def _on_assign(self, consumer, partitions):
        logger.info("partitions_assigned",
                     partitions=[(p.topic, p.partition) for p in partitions])


class TopicManager:
    """Kafka topic administration."""

    def __init__(self, config: KafkaConfig):
        self._admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})

    def create_topics(self, topics: list[dict]):
        new_topics = [
            NewTopic(t["name"], num_partitions=t.get("partitions", 6),
                     replication_factor=t.get("replication", 1),
                     config=t.get("config", {}))
            for t in topics
        ]
        futures = self._admin.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info("topic_created", topic=topic)
            except KafkaException as e:
                if "already exists" in str(e):
                    logger.info("topic_exists", topic=topic)
                else:
                    logger.error("topic_creation_failed", topic=topic, error=str(e))

    def ensure_topics(self, config: KafkaConfig):
        topics = [
            {"name": config.raw_events_topic, "partitions": 6,
             "config": {"retention.ms": "604800000"}},  # 7 days
            {"name": config.enriched_events_topic, "partitions": 6,
             "config": {"retention.ms": "604800000"}},
            {"name": config.alerts_topic, "partitions": 3,
             "config": {"retention.ms": "2592000000"}},  # 30 days
            {"name": config.metrics_topic, "partitions": 3,
             "config": {"retention.ms": "86400000"}},  # 1 day
            {"name": config.deadletter_topic, "partitions": 1,
             "config": {"retention.ms": "-1"}},  # infinite
        ]
        self.create_topics(topics)
