"""Tests for Complex Event Processing engine."""

import pytest
from datetime import datetime, timedelta
from src.processors.cep_engine import CEPEngine
from src.utils.state_store import StateStore


@pytest.fixture
def cep():
    state = StateStore("test_cep", ttl_seconds=3600)
    return CEPEngine(state, config={
        "velocity_threshold": 5,
        "amount_spike_threshold": 3.0,
        "geo_velocity_kmh": 900.0,
        "rapid_fire_count": 3,
        "rapid_fire_window_seconds": 10,
    })


class TestVelocityPattern:
    def test_triggers_on_high_velocity(self, cep):
        base = datetime(2024, 1, 1, 12, 0, 0)
        alerts = []
        for i in range(6):
            result = cep.evaluate("user1", {
                "event_id": f"E{i}", "amount": 10.0, "merchant_id": "M1",
                "timestamp": (base + timedelta(minutes=i)).isoformat(),
            })
            alerts.extend(result)
        velocity_alerts = [a for a in alerts if a.pattern_name == "velocity_spike"]
        assert len(velocity_alerts) > 0

    def test_no_alert_below_threshold(self, cep):
        base = datetime(2024, 1, 1, 12, 0, 0)
        alerts = cep.evaluate("user2", {
            "event_id": "E1", "amount": 10.0, "merchant_id": "M1",
            "timestamp": base.isoformat(),
        })
        velocity_alerts = [a for a in alerts if a.pattern_name == "velocity_spike"]
        assert len(velocity_alerts) == 0


class TestAmountAnomaly:
    def test_detects_amount_spike(self, cep):
        base = datetime(2024, 1, 1, 12, 0, 0)
        # Build baseline (10 normal events)
        for i in range(15):
            cep.evaluate("user3", {
                "event_id": f"N{i}", "amount": 50.0, "merchant_id": "M1",
                "timestamp": (base + timedelta(minutes=i)).isoformat(),
            })
        # Spike
        alerts = cep.evaluate("user3", {
            "event_id": "SPIKE", "amount": 5000.0, "merchant_id": "M1",
            "timestamp": (base + timedelta(minutes=20)).isoformat(),
        })
        anomaly_alerts = [a for a in alerts if a.pattern_name == "amount_anomaly"]
        assert len(anomaly_alerts) > 0
        assert anomaly_alerts[0].context["zscore"] > 3.0


class TestImpossibleTravel:
    def test_detects_impossible_travel(self, cep):
        base = datetime(2024, 1, 1, 12, 0, 0)
        # Event in New York
        cep.evaluate("user4", {
            "event_id": "E1", "amount": 10.0, "merchant_id": "M1",
            "timestamp": base.isoformat(),
            "location_lat": 40.7128, "location_lon": -74.0060,
        })
        # Event in London 5 minutes later (impossible)
        alerts = cep.evaluate("user4", {
            "event_id": "E2", "amount": 20.0, "merchant_id": "M2",
            "timestamp": (base + timedelta(minutes=5)).isoformat(),
            "location_lat": 51.5074, "location_lon": -0.1278,
        })
        travel_alerts = [a for a in alerts if a.pattern_name == "impossible_travel"]
        assert len(travel_alerts) > 0
        assert travel_alerts[0].severity.value == "critical"


class TestRapidFire:
    def test_detects_rapid_burst(self, cep):
        base = datetime(2024, 1, 1, 12, 0, 0)
        alerts = []
        for i in range(4):
            result = cep.evaluate("user5", {
                "event_id": f"R{i}", "amount": 5.0, "merchant_id": "M1",
                "timestamp": (base + timedelta(seconds=i * 2)).isoformat(),
            })
            alerts.extend(result)
        rapid_alerts = [a for a in alerts if a.pattern_name == "rapid_fire"]
        assert len(rapid_alerts) > 0


class TestHaversine:
    def test_known_distance(self):
        # NY to London ≈ 5570 km
        dist = CEPEngine._haversine(40.7128, -74.0060, 51.5074, -0.1278)
        assert 5500 < dist < 5700

    def test_same_point(self):
        dist = CEPEngine._haversine(40.0, -74.0, 40.0, -74.0)
        assert dist < 0.001
