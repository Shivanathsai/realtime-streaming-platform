"""Tests for state store and event models."""

import pytest
import time
from datetime import datetime
from src.utils.state_store import StateStore
from src.models.events import RawEvent, EventType, TransactionEvent, Alert, AlertSeverity


class TestStateStore:
    def test_put_and_get(self):
        store = StateStore("test", ttl_seconds=60)
        store.put("key1", {"value": 42})
        assert store.get("key1") == {"value": 42}

    def test_get_missing_key(self):
        store = StateStore("test", ttl_seconds=60)
        assert store.get("missing") is None

    def test_delete(self):
        store = StateStore("test", ttl_seconds=60)
        store.put("key1", {"value": 1})
        store.delete("key1")
        assert store.get("key1") is None

    def test_get_or_default(self):
        store = StateStore("test", ttl_seconds=60)
        val = store.get_or_default("new_key", lambda: {"count": 0})
        assert val == {"count": 0}
        assert store.get("new_key") == {"count": 0}

    def test_update(self):
        store = StateStore("test", ttl_seconds=60)
        store.put("key1", {"count": 5})
        result = store.update("key1", lambda d: {**d, "count": d["count"] + 1})
        assert result["count"] == 6

    def test_size(self):
        store = StateStore("test", ttl_seconds=60)
        store.put("a", {"v": 1})
        store.put("b", {"v": 2})
        assert store.size() == 2

    def test_ttl_expiry(self):
        store = StateStore("test", ttl_seconds=1)
        store.put("key1", {"value": 1})
        time.sleep(1.1)
        assert store.get("key1") is None


class TestEventModels:
    def test_raw_event_serialize_deserialize(self):
        event = RawEvent(
            event_type=EventType.TRANSACTION,
            user_id="USR001",
            properties={"amount": 99.99, "merchant": "test"},
        )
        data = event.serialize()
        restored = RawEvent.deserialize(data)
        assert restored.user_id == "USR001"
        assert restored.properties["amount"] == 99.99
        assert restored.event_type == EventType.TRANSACTION

    def test_transaction_event_fields(self):
        txn = TransactionEvent(
            event_id="TXN001", timestamp=datetime.utcnow(),
            user_id="USR001", amount=150.0, merchant_id="MCH001",
            merchant_category="grocery", payment_method="credit_card",
        )
        data = txn.serialize()
        restored = TransactionEvent.deserialize(data)
        assert restored.amount == 150.0
        assert restored.merchant_category == "grocery"

    def test_alert_model(self):
        alert = Alert(
            severity=AlertSeverity.HIGH,
            pattern_name="velocity_spike",
            user_id="USR001",
            description="Test alert",
            context={"count": 10},
        )
        data = alert.serialize()
        restored = Alert.deserialize(data)
        assert restored.severity == AlertSeverity.HIGH
        assert restored.pattern_name == "velocity_spike"
        assert restored.context["count"] == 10
