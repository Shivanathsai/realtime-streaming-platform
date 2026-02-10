"""Windowing functions — tumbling, sliding, and session windows for stream aggregation."""

import time
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Callable, Optional, Dict, Any, List

from src.models.events import WindowAggregate, TransactionEvent
from src.utils.logging import get_logger
from src.utils import metrics as m

logger = get_logger(__name__)


class TumblingWindow:
    """Fixed-size, non-overlapping time window.

    Events are grouped into discrete windows of `window_seconds` duration.
    Each event belongs to exactly one window.
    """

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds
        self._buffers: Dict[str, Dict[int, List[Dict]]] = defaultdict(lambda: defaultdict(list))

    def add(self, key: str, event: Dict[str, Any], timestamp: datetime) -> Optional[WindowAggregate]:
        window_id = int(timestamp.timestamp()) // self.window_seconds
        self._buffers[key][window_id].append(event)

        # Check if previous window is ready to emit
        prev_id = window_id - 1
        if prev_id in self._buffers[key] and len(self._buffers[key][prev_id]) > 0:
            result = self._emit(key, prev_id)
            del self._buffers[key][prev_id]
            m.window_aggregations.labels(window_type="tumbling").inc()
            return result
        return None

    def _emit(self, key: str, window_id: int) -> WindowAggregate:
        events = self._buffers[key][window_id]
        start = datetime.utcfromtimestamp(window_id * self.window_seconds)
        end = start + timedelta(seconds=self.window_seconds)
        amounts = [e.get("amount", 0) for e in events]
        merchants = set(e.get("merchant_id", "") for e in events)

        return WindowAggregate(
            window_start=start,
            window_end=end,
            window_type="tumbling",
            group_key=key,
            event_count=len(events),
            sum_amount=sum(amounts),
            avg_amount=sum(amounts) / max(len(amounts), 1),
            min_amount=min(amounts) if amounts else 0,
            max_amount=max(amounts) if amounts else 0,
            distinct_merchants=len(merchants),
        )


class SlidingWindow:
    """Overlapping time window that slides by `step_seconds`.

    Produces aggregates more frequently than tumbling windows, capturing
    trends that span window boundaries.
    """

    def __init__(self, window_seconds: int = 300, step_seconds: int = 60):
        self.window_seconds = window_seconds
        self.step_seconds = step_seconds
        self._buffers: Dict[str, List[Dict]] = defaultdict(list)
        self._last_emit: Dict[str, float] = {}

    def add(self, key: str, event: Dict[str, Any], timestamp: datetime) -> Optional[WindowAggregate]:
        ts = timestamp.timestamp()
        event["_ts"] = ts
        self._buffers[key].append(event)

        # Evict expired events
        cutoff = ts - self.window_seconds
        self._buffers[key] = [e for e in self._buffers[key] if e["_ts"] >= cutoff]

        # Emit on step interval
        last = self._last_emit.get(key, 0)
        if ts - last >= self.step_seconds:
            self._last_emit[key] = ts
            m.window_aggregations.labels(window_type="sliding").inc()
            return self._emit(key, timestamp)
        return None

    def _emit(self, key: str, now: datetime) -> WindowAggregate:
        events = self._buffers[key]
        amounts = [e.get("amount", 0) for e in events]
        merchants = set(e.get("merchant_id", "") for e in events)

        return WindowAggregate(
            window_start=now - timedelta(seconds=self.window_seconds),
            window_end=now,
            window_type="sliding",
            group_key=key,
            event_count=len(events),
            sum_amount=sum(amounts),
            avg_amount=sum(amounts) / max(len(amounts), 1),
            min_amount=min(amounts) if amounts else 0,
            max_amount=max(amounts) if amounts else 0,
            distinct_merchants=len(merchants),
        )


class SessionWindow:
    """Activity-based window that groups events by session gaps.

    A session ends when no event arrives within `gap_seconds`.
    Captures natural user activity bursts.
    """

    def __init__(self, gap_seconds: int = 120):
        self.gap_seconds = gap_seconds
        self._sessions: Dict[str, Dict] = {}

    def add(self, key: str, event: Dict[str, Any], timestamp: datetime) -> Optional[WindowAggregate]:
        ts = timestamp.timestamp()
        session = self._sessions.get(key)

        if session and (ts - session["last_ts"]) > self.gap_seconds:
            # Session expired — emit and start new
            result = self._emit(key, session)
            self._sessions[key] = {"start": ts, "last_ts": ts, "events": [event]}
            m.window_aggregations.labels(window_type="session").inc()
            return result

        if session:
            session["last_ts"] = ts
            session["events"].append(event)
        else:
            self._sessions[key] = {"start": ts, "last_ts": ts, "events": [event]}

        return None

    def _emit(self, key: str, session: Dict) -> WindowAggregate:
        events = session["events"]
        amounts = [e.get("amount", 0) for e in events]
        merchants = set(e.get("merchant_id", "") for e in events)

        return WindowAggregate(
            window_start=datetime.utcfromtimestamp(session["start"]),
            window_end=datetime.utcfromtimestamp(session["last_ts"]),
            window_type="session",
            group_key=key,
            event_count=len(events),
            sum_amount=sum(amounts),
            avg_amount=sum(amounts) / max(len(amounts), 1),
            min_amount=min(amounts) if amounts else 0,
            max_amount=max(amounts) if amounts else 0,
            distinct_merchants=len(merchants),
        )

    def flush_expired(self, now: datetime) -> List[WindowAggregate]:
        """Emit all expired sessions. Called periodically."""
        results = []
        ts = now.timestamp()
        expired_keys = [
            k for k, s in self._sessions.items()
            if (ts - s["last_ts"]) > self.gap_seconds
        ]
        for key in expired_keys:
            session = self._sessions.pop(key)
            results.append(self._emit(key, session))
            m.window_aggregations.labels(window_type="session").inc()
        return results
