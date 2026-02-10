"""Stateful processing with Redis-backed state store and local fallback."""

import time
import json
from typing import Optional, Dict, Any
from collections import defaultdict

from src.utils.logging import get_logger

logger = get_logger(__name__)


class StateStore:
    """In-memory state store with optional Redis persistence for fault tolerance."""

    def __init__(self, name: str, ttl_seconds: int = 86400, redis_client=None):
        self.name = name
        self.ttl = ttl_seconds
        self._local: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._timestamps: Dict[str, float] = {}
        self._redis = redis_client
        logger.info("state_store_initialized", name=name, backend="redis" if redis_client else "local")

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        self._evict_expired(key)
        if self._redis:
            raw = self._redis.get(f"{self.name}:{key}")
            if raw:
                return json.loads(raw)
        return self._local.get(key)

    def put(self, key: str, value: Dict[str, Any]):
        self._local[key] = value
        self._timestamps[key] = time.time()
        if self._redis:
            self._redis.setex(f"{self.name}:{key}", self.ttl, json.dumps(value, default=str))

    def delete(self, key: str):
        self._local.pop(key, None)
        self._timestamps.pop(key, None)
        if self._redis:
            self._redis.delete(f"{self.name}:{key}")

    def get_or_default(self, key: str, default_factory) -> Dict[str, Any]:
        value = self.get(key)
        if value is None:
            value = default_factory()
            self.put(key, value)
        return value

    def update(self, key: str, update_fn):
        current = self.get_or_default(key, dict)
        updated = update_fn(current)
        self.put(key, updated)
        return updated

    def keys(self) -> list[str]:
        return list(self._local.keys())

    def size(self) -> int:
        return len(self._local)

    def _evict_expired(self, key: str):
        ts = self._timestamps.get(key, 0)
        if ts and (time.time() - ts) > self.ttl:
            self._local.pop(key, None)
            self._timestamps.pop(key, None)

    def flush(self):
        """Persist all local state to Redis (checkpoint)."""
        if not self._redis:
            return
        pipe = self._redis.pipeline()
        for key, value in self._local.items():
            pipe.setex(f"{self.name}:{key}", self.ttl, json.dumps(value, default=str))
        pipe.execute()
        logger.info("state_flushed", store=self.name, keys=len(self._local))

    def restore(self):
        """Restore state from Redis after restart (recovery)."""
        if not self._redis:
            return
        cursor = 0
        prefix = f"{self.name}:"
        while True:
            cursor, keys = self._redis.scan(cursor, match=f"{prefix}*", count=1000)
            for key in keys:
                raw = self._redis.get(key)
                if raw:
                    local_key = key.decode().removeprefix(prefix)
                    self._local[local_key] = json.loads(raw)
                    self._timestamps[local_key] = time.time()
            if cursor == 0:
                break
        logger.info("state_restored", store=self.name, keys=len(self._local))
