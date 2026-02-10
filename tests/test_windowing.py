"""Tests for windowing functions."""

import pytest
from datetime import datetime, timedelta
from src.processors.windowing import TumblingWindow, SlidingWindow, SessionWindow


class TestTumblingWindow:
    def test_emits_on_window_boundary(self):
        w = TumblingWindow(window_seconds=60)
        base = datetime(2024, 1, 1, 12, 0, 0)
        # Fill first window
        for i in range(5):
            w.add("user1", {"amount": 10.0, "merchant_id": f"M{i}"}, base + timedelta(seconds=i * 10))
        # Trigger emit by entering next window
        result = w.add("user1", {"amount": 20.0, "merchant_id": "M0"}, base + timedelta(seconds=65))
        assert result is not None
        assert result.event_count == 5
        assert result.sum_amount == 50.0
        assert result.window_type == "tumbling"

    def test_no_emit_within_same_window(self):
        w = TumblingWindow(window_seconds=60)
        base = datetime(2024, 1, 1, 12, 0, 0)
        result = w.add("user1", {"amount": 10.0}, base + timedelta(seconds=5))
        assert result is None

    def test_distinct_merchants(self):
        w = TumblingWindow(window_seconds=60)
        base = datetime(2024, 1, 1, 12, 0, 0)
        for i in range(3):
            w.add("user1", {"amount": 10.0, "merchant_id": f"M{i}"}, base + timedelta(seconds=i))
        result = w.add("user1", {"amount": 10.0}, base + timedelta(seconds=65))
        assert result.distinct_merchants == 3


class TestSlidingWindow:
    def test_emits_on_step_interval(self):
        w = SlidingWindow(window_seconds=300, step_seconds=60)
        base = datetime(2024, 1, 1, 12, 0, 0)
        # First event triggers initial emit (last_emit starts at 0)
        first = w.add("user1", {"amount": 5.0}, base)
        assert first is not None  # first event always emits (0 -> now > step)
        # Events within step interval should not emit
        result = w.add("user1", {"amount": 5.0}, base + timedelta(seconds=30))
        assert result is None
        # Event past the step boundary emits
        result = w.add("user1", {"amount": 5.0}, base + timedelta(seconds=65))
        assert result is not None
        assert result.window_type == "sliding"
        assert result.event_count == 3

    def test_evicts_expired_events(self):
        w = SlidingWindow(window_seconds=60, step_seconds=30)
        base = datetime(2024, 1, 1, 12, 0, 0)
        w.add("user1", {"amount": 10.0}, base)
        # Add event well past the window
        result = w.add("user1", {"amount": 20.0}, base + timedelta(seconds=120))
        if result:
            assert result.event_count == 1  # old event evicted


class TestSessionWindow:
    def test_session_emits_on_gap(self):
        w = SessionWindow(gap_seconds=30)
        base = datetime(2024, 1, 1, 12, 0, 0)
        w.add("user1", {"amount": 10.0}, base)
        w.add("user1", {"amount": 20.0}, base + timedelta(seconds=10))
        # Gap > 30s triggers emit of previous session
        result = w.add("user1", {"amount": 30.0}, base + timedelta(seconds=50))
        assert result is not None
        assert result.event_count == 2
        assert result.sum_amount == 30.0
        assert result.window_type == "session"

    def test_no_emit_within_session(self):
        w = SessionWindow(gap_seconds=60)
        base = datetime(2024, 1, 1, 12, 0, 0)
        result = w.add("user1", {"amount": 10.0}, base)
        assert result is None
        result = w.add("user1", {"amount": 20.0}, base + timedelta(seconds=30))
        assert result is None

    def test_flush_expired(self):
        w = SessionWindow(gap_seconds=30)
        base = datetime(2024, 1, 1, 12, 0, 0)
        w.add("user1", {"amount": 10.0}, base)
        w.add("user2", {"amount": 20.0}, base + timedelta(seconds=5))
        results = w.flush_expired(base + timedelta(seconds=60))
        assert len(results) == 2
