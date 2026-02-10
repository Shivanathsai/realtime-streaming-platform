"""Event producer — generates realistic transaction streams at configurable throughput."""

import time
import random
import math
from datetime import datetime

from config.settings import get_config
from src.models.events import RawEvent, EventType, TransactionEvent
from src.utils.kafka_client import EventProducer, TopicManager
from src.utils.logging import setup_logging, get_logger
from src.utils import metrics as m

logger = get_logger(__name__)

MERCHANTS = [
    ("MCH001", "grocery", "Whole Foods"), ("MCH002", "grocery", "Trader Joes"),
    ("MCH003", "gas", "Shell"), ("MCH004", "gas", "Chevron"),
    ("MCH005", "restaurant", "Chipotle"), ("MCH006", "restaurant", "Starbucks"),
    ("MCH007", "electronics", "Best Buy"), ("MCH008", "electronics", "Apple Store"),
    ("MCH009", "travel", "United Airlines"), ("MCH010", "travel", "Marriott"),
    ("MCH011", "online", "Amazon"), ("MCH012", "online", "Netflix"),
    ("MCH013", "retail", "Target"), ("MCH014", "retail", "Walmart"),
    ("MCH015", "healthcare", "CVS Pharmacy"),
]

LOCATIONS = [
    (40.7128, -74.0060, "New York"), (34.0522, -118.2437, "Los Angeles"),
    (41.8781, -87.6298, "Chicago"), (29.7604, -95.3698, "Houston"),
    (33.4484, -112.0740, "Phoenix"), (39.7392, -104.9903, "Denver"),
    (47.6062, -122.3321, "Seattle"), (25.7617, -80.1918, "Miami"),
    (51.5074, -0.1278, "London"), (48.8566, 2.3522, "Paris"),
]

PAYMENT_METHODS = ["credit_card", "debit_card", "mobile_pay", "wire_transfer"]


class TransactionGenerator:
    """Generates realistic transaction event streams with anomaly injection."""

    def __init__(self, num_users: int = 10000, anomaly_rate: float = 0.02):
        self.num_users = num_users
        self.anomaly_rate = anomaly_rate
        self.user_profiles = self._build_profiles()

    def _build_profiles(self) -> dict:
        profiles = {}
        for i in range(self.num_users):
            uid = f"USR{str(i).zfill(6)}"
            home_loc = random.choice(LOCATIONS)
            profiles[uid] = {
                "avg_amount": random.uniform(15, 200),
                "std_amount": random.uniform(5, 50),
                "home_location": home_loc,
                "preferred_merchants": random.sample(MERCHANTS, k=random.randint(3, 8)),
                "txn_frequency_per_hour": random.uniform(0.5, 5),
            }
        return profiles

    def generate_event(self) -> TransactionEvent:
        user_id = f"USR{str(random.randint(0, self.num_users - 1)).zfill(6)}"
        profile = self.user_profiles[user_id]
        is_anomaly = random.random() < self.anomaly_rate

        if is_anomaly:
            return self._generate_anomalous(user_id, profile)
        return self._generate_normal(user_id, profile)

    def _generate_normal(self, user_id: str, profile: dict) -> TransactionEvent:
        merchant = random.choice(profile["preferred_merchants"])
        loc = profile["home_location"]
        amount = max(0.5, random.gauss(profile["avg_amount"], profile["std_amount"]))

        return TransactionEvent(
            event_id=f"TXN{int(time.time() * 1000000)}",
            timestamp=datetime.utcnow(),
            user_id=user_id,
            amount=round(amount, 2),
            merchant_id=merchant[0],
            merchant_category=merchant[1],
            payment_method=random.choice(PAYMENT_METHODS),
            location_lat=loc[0] + random.gauss(0, 0.01),
            location_lon=loc[1] + random.gauss(0, 0.01),
            device_id=f"DEV{hash(user_id) % 10000:04d}",
            is_international=False,
        )

    def _generate_anomalous(self, user_id: str, profile: dict) -> TransactionEvent:
        anomaly_type = random.choice(["high_amount", "impossible_travel", "unusual_merchant", "rapid_burst"])

        if anomaly_type == "high_amount":
            amount = profile["avg_amount"] * random.uniform(5, 20)
            loc = profile["home_location"]
        elif anomaly_type == "impossible_travel":
            amount = max(0.5, random.gauss(profile["avg_amount"], profile["std_amount"]))
            loc = random.choice([l for l in LOCATIONS if l != profile["home_location"]])
        elif anomaly_type == "unusual_merchant":
            amount = max(0.5, random.gauss(profile["avg_amount"], profile["std_amount"]))
            loc = profile["home_location"]
        else:  # rapid_burst
            amount = random.uniform(1, 50)
            loc = profile["home_location"]

        merchant = random.choice(MERCHANTS)
        return TransactionEvent(
            event_id=f"TXN{int(time.time() * 1000000)}",
            timestamp=datetime.utcnow(),
            user_id=user_id,
            amount=round(amount, 2),
            merchant_id=merchant[0],
            merchant_category=merchant[1],
            payment_method=random.choice(PAYMENT_METHODS),
            location_lat=loc[0] + random.gauss(0, 0.01),
            location_lon=loc[1] + random.gauss(0, 0.01),
            device_id=f"DEV{hash(user_id) % 10000:04d}",
            is_international=(loc != profile["home_location"]),
        )


def run_producer(events_per_second: int = 500, duration_seconds: int = None):
    """Main producer loop — generates events at target throughput."""
    setup_logging()
    config = get_config()

    topic_mgr = TopicManager(config.kafka)
    topic_mgr.ensure_topics(config.kafka)

    producer = EventProducer(config.kafka)
    generator = TransactionGenerator(num_users=10000, anomaly_rate=0.02)

    logger.info("producer_started", target_eps=events_per_second, duration=duration_seconds)
    start_time = time.time()
    total_sent = 0
    batch_start = time.time()

    try:
        while True:
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                break

            event = generator.generate_event()

            # Wrap as RawEvent envelope
            raw = RawEvent(
                event_id=event.event_id,
                event_type=EventType.TRANSACTION,
                timestamp=event.timestamp,
                user_id=event.user_id,
                properties=event.model_dump(exclude={"event_id", "timestamp", "user_id"}),
            )

            producer.produce(
                topic=config.kafka.raw_events_topic,
                key=event.user_id,
                value=raw.serialize(),
                headers={"event_type": "transaction", "source": "generator"},
            )
            total_sent += 1

            # Throttle to target EPS
            elapsed = time.time() - batch_start
            expected = total_sent / events_per_second
            if expected > elapsed:
                time.sleep(expected - elapsed)

            # Log throughput every 5 seconds
            if total_sent % (events_per_second * 5) == 0:
                actual_eps = total_sent / (time.time() - start_time)
                m.pipeline_throughput.set(actual_eps)
                logger.info("producer_throughput",
                            total=total_sent, eps=round(actual_eps, 1))

    except KeyboardInterrupt:
        logger.info("producer_interrupted")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        logger.info("producer_finished",
                     total_events=total_sent,
                     duration_seconds=round(elapsed, 1),
                     avg_eps=round(total_sent / max(elapsed, 0.001), 1))


if __name__ == "__main__":
    import click

    @click.command()
    @click.option("--eps", default=500, help="Events per second")
    @click.option("--duration", default=None, type=int, help="Duration in seconds (None=infinite)")
    def main(eps, duration):
        run_producer(events_per_second=eps, duration_seconds=duration)

    main()
