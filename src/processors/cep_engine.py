"""Complex Event Processing (CEP) — pattern detection for anomalies and business rules."""

import math
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from src.models.events import Alert, AlertSeverity, TransactionEvent
from src.utils.state_store import StateStore
from src.utils.logging import get_logger
from src.utils import metrics as m

logger = get_logger(__name__)


class CEPEngine:
    """Stateful CEP engine detecting multi-event patterns across streams.

    Patterns implemented:
    1. Velocity spike — too many events in a short window
    2. Amount anomaly — transaction amount far from user's baseline
    3. Impossible travel — geographically impossible consecutive locations
    4. Rapid-fire — burst of events faster than human speed
    5. Merchant diversity spike — sudden use of many new merchants
    """

    def __init__(self, state: StateStore, config=None):
        self.state = state
        self.config = config or {}
        self.velocity_threshold = self.config.get("velocity_threshold", 10)
        self.amount_spike_std = self.config.get("amount_spike_threshold", 3.0)
        self.geo_velocity_kmh = self.config.get("geo_velocity_kmh", 900.0)
        self.rapid_fire_count = self.config.get("rapid_fire_count", 5)
        self.rapid_fire_window = self.config.get("rapid_fire_window_seconds", 10)
        logger.info("cep_engine_initialized", patterns=5)

    def evaluate(self, user_id: str, event: Dict[str, Any]) -> List[Alert]:
        """Run all CEP patterns against the incoming event. Returns alerts."""
        alerts = []
        user_state = self.state.get_or_default(user_id, lambda: {
            "events": [], "amounts": [], "locations": [],
            "running_mean": 0, "running_m2": 0, "count": 0,
        })

        ts = event.get("timestamp", datetime.utcnow().isoformat())
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00").replace("+00:00", ""))
        amount = event.get("amount", 0)

        # Update rolling stats (Welford's online algorithm)
        user_state["count"] += 1
        n = user_state["count"]
        delta = amount - user_state["running_mean"]
        user_state["running_mean"] += delta / n
        delta2 = amount - user_state["running_mean"]
        user_state["running_m2"] += delta * delta2

        # Keep recent events (last 1h)
        cutoff = (ts - timedelta(hours=1)).isoformat() if isinstance(ts, datetime) else ts
        user_state["events"] = [
            e for e in user_state["events"]
            if e.get("timestamp", "") > str(cutoff)
        ][-200:]  # cap buffer size
        user_state["events"].append({
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts),
            "amount": amount,
            "merchant_id": event.get("merchant_id", ""),
            "lat": event.get("location_lat"),
            "lon": event.get("location_lon"),
            "event_id": event.get("event_id", ""),
        })

        # Pattern 1: Velocity spike
        alert = self._check_velocity(user_id, user_state, ts)
        if alert:
            alerts.append(alert)

        # Pattern 2: Amount anomaly
        alert = self._check_amount_anomaly(user_id, user_state, event, amount)
        if alert:
            alerts.append(alert)

        # Pattern 3: Impossible travel
        alert = self._check_impossible_travel(user_id, user_state, event, ts)
        if alert:
            alerts.append(alert)

        # Pattern 4: Rapid-fire
        alert = self._check_rapid_fire(user_id, user_state, ts)
        if alert:
            alerts.append(alert)

        # Pattern 5: Merchant diversity spike
        alert = self._check_merchant_diversity(user_id, user_state)
        if alert:
            alerts.append(alert)

        # Persist updated state
        self.state.put(user_id, user_state)

        for a in alerts:
            m.alerts_generated.labels(severity=a.severity.value, pattern=a.pattern_name).inc()

        return alerts

    def _check_velocity(self, user_id: str, state: Dict, ts: datetime) -> Optional[Alert]:
        recent = state["events"]
        if len(recent) >= self.velocity_threshold:
            return Alert(
                severity=AlertSeverity.HIGH,
                pattern_name="velocity_spike",
                user_id=user_id,
                description=f"User {user_id} generated {len(recent)} events in 1h "
                            f"(threshold: {self.velocity_threshold})",
                trigger_events=[e["event_id"] for e in recent[-5:]],
                context={"event_count": len(recent), "threshold": self.velocity_threshold},
            )
        return None

    def _check_amount_anomaly(self, user_id: str, state: Dict,
                               event: Dict, amount: float) -> Optional[Alert]:
        if state["count"] < 10:
            return None  # not enough history
        mean = state["running_mean"]
        variance = state["running_m2"] / state["count"]
        std = math.sqrt(max(variance, 0.01))
        zscore = (amount - mean) / std

        if abs(zscore) > self.amount_spike_std:
            return Alert(
                severity=AlertSeverity.HIGH if zscore > 5 else AlertSeverity.MEDIUM,
                pattern_name="amount_anomaly",
                user_id=user_id,
                description=f"Transaction ${amount:.2f} is {zscore:.1f} std devs from "
                            f"user mean ${mean:.2f} (std ${std:.2f})",
                trigger_events=[event.get("event_id", "")],
                context={"amount": amount, "mean": round(mean, 2),
                          "std": round(std, 2), "zscore": round(zscore, 2)},
            )
        return None

    def _check_impossible_travel(self, user_id: str, state: Dict,
                                  event: Dict, ts: datetime) -> Optional[Alert]:
        events = state["events"]
        if len(events) < 2:
            return None

        prev = events[-2]
        curr_lat, curr_lon = event.get("location_lat"), event.get("location_lon")
        prev_lat, prev_lon = prev.get("lat"), prev.get("lon")

        if not all([curr_lat, curr_lon, prev_lat, prev_lon]):
            return None

        distance_km = self._haversine(prev_lat, prev_lon, curr_lat, curr_lon)
        prev_ts = datetime.fromisoformat(prev["timestamp"]) if isinstance(prev["timestamp"], str) else prev["timestamp"]
        time_diff_hours = max((ts - prev_ts).total_seconds() / 3600, 0.001)
        speed_kmh = distance_km / time_diff_hours

        if speed_kmh > self.geo_velocity_kmh and distance_km > 50:
            return Alert(
                severity=AlertSeverity.CRITICAL,
                pattern_name="impossible_travel",
                user_id=user_id,
                description=f"Impossible travel: {distance_km:.0f} km in "
                            f"{time_diff_hours * 60:.0f} min ({speed_kmh:.0f} km/h)",
                trigger_events=[prev.get("event_id", ""), event.get("event_id", "")],
                context={"distance_km": round(distance_km, 1),
                          "speed_kmh": round(speed_kmh, 1),
                          "time_diff_min": round(time_diff_hours * 60, 1)},
            )
        return None

    def _check_rapid_fire(self, user_id: str, state: Dict, ts: datetime) -> Optional[Alert]:
        cutoff = ts - timedelta(seconds=self.rapid_fire_window)
        recent = [
            e for e in state["events"]
            if e.get("timestamp", "") > cutoff.isoformat()
        ]
        if len(recent) >= self.rapid_fire_count:
            return Alert(
                severity=AlertSeverity.HIGH,
                pattern_name="rapid_fire",
                user_id=user_id,
                description=f"{len(recent)} events in {self.rapid_fire_window}s "
                            f"(threshold: {self.rapid_fire_count})",
                trigger_events=[e["event_id"] for e in recent],
                context={"count": len(recent), "window_seconds": self.rapid_fire_window},
            )
        return None

    def _check_merchant_diversity(self, user_id: str, state: Dict) -> Optional[Alert]:
        events = state["events"]
        if len(events) < 5:
            return None
        recent_merchants = set(e.get("merchant_id", "") for e in events[-10:])
        if len(recent_merchants) >= 8:
            return Alert(
                severity=AlertSeverity.MEDIUM,
                pattern_name="merchant_diversity_spike",
                user_id=user_id,
                description=f"User transacted at {len(recent_merchants)} different "
                            f"merchants in recent activity",
                trigger_events=[events[-1].get("event_id", "")],
                context={"distinct_merchants": len(recent_merchants)},
            )
        return None

    @staticmethod
    def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371  # Earth radius km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon / 2) ** 2)
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
