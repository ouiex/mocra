"""Distributed sliding window rate limiter (port of Rust utils::distributed_rate_limit)."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Dict, Optional

from redis.asyncio import Redis

from .redis_lock import DistributedLockManager


@dataclass
class RateLimitConfig:
    max_requests_per_second: float = 2.0
    window_size_millis: int = 1000
    base_max_requests_per_second: Optional[float] = None

    def with_window_size(self, window_seconds: float) -> "RateLimitConfig":
        self.window_size_millis = int(window_seconds * 1000)
        return self


class DistributedSlidingWindowRateLimiter:
    def __init__(
        self,
        redis: Optional[Redis],
        locker: Optional[DistributedLockManager],
        namespace: str,
        default_config: RateLimitConfig,
    ) -> None:
        self.redis = redis
        self.lock_manager = locker
        self.sub_prefix = "rate_limiter"
        self.key_prefix = f"{namespace}:{self.sub_prefix}"
        self.default_config_key = f"{self.key_prefix}:default_config"
        self.config_key_prefix = f"{self.key_prefix}:config"
        self.default_config = default_config

        self._local_last_request: Dict[str, int] = {}
        self._local_configs: Dict[str, RateLimitConfig] = {}
        self._local_default_config: Optional[RateLimitConfig] = None
        self._local_suspended: Dict[str, int] = {}
        self._local_wait_until: Dict[str, int] = {}
        self._time_offset: Optional[tuple[int, float]] = None
        self._suspend_cache: Dict[str, tuple[float, Optional[int]]] = {}
        self._lock = asyncio.Lock()

    def _get_last_request_key(self, identifier: str) -> str:
        return f"{self.key_prefix}:last_request:{identifier}"

    def _get_suspended_key(self, identifier: str) -> str:
        return f"{self.key_prefix}:suspended:{identifier}"

    def _get_config_key(self, identifier: str) -> str:
        return f"{self.config_key_prefix}:{identifier}"

    async def _current_timestamp(self) -> int:
        if self.redis is None:
            return int(time.time() * 1000)

        if self._time_offset is not None:
            offset, last_refresh = self._time_offset
            if time.time() - last_refresh < 60:
                local_ms = int(time.time() * 1000)
                return local_ms - offset

        try:
            seconds, microseconds = await self.redis.time()
            redis_ms = int(seconds * 1000 + microseconds / 1000)
            local_ms = int(time.time() * 1000)
            self._time_offset = (local_ms - redis_ms, time.time())
            return redis_ms
        except Exception:
            return int(time.time() * 1000)

    async def set_default_config(self, config: RateLimitConfig) -> None:
        self.default_config = config
        self._local_default_config = config
        if self.redis:
            await self.redis.set(self.default_config_key, json.dumps(config.__dict__))

    async def set_config(self, identifier: str, config: RateLimitConfig) -> None:
        self._local_configs[identifier] = config
        if self.redis:
            await self.redis.set(self._get_config_key(identifier), json.dumps(config.__dict__))

    async def _get_config(self, identifier: str) -> RateLimitConfig:
        if identifier in self._local_configs:
            return self._local_configs[identifier]

        if self.redis:
            raw = await self.redis.get(self._get_config_key(identifier))
            if raw:
                data = json.loads(raw)
                config = RateLimitConfig(**data)
                self._local_configs[identifier] = config
                return config

        if self._local_default_config:
            return self._local_default_config

        if self.redis:
            raw = await self.redis.get(self.default_config_key)
            if raw:
                data = json.loads(raw)
                self._local_default_config = RateLimitConfig(**data)
                return self._local_default_config

        return self.default_config

    async def suspend(self, identifier: str, duration_seconds: float) -> None:
        now = await self._current_timestamp()
        suspend_until = now + int(duration_seconds * 1000)
        if self.redis:
            await self.redis.set(self._get_suspended_key(identifier), suspend_until, ex=int(duration_seconds))
        else:
            self._local_suspended[identifier] = suspend_until
        self._suspend_cache.pop(identifier, None)

    async def _check_suspended(self, identifier: str) -> Optional[int]:
        cached = self._suspend_cache.get(identifier)
        if cached:
            checked_at, suspend_until = cached
            if time.time() - checked_at < 0.5:
                if suspend_until is None:
                    return None
                now = await self._current_timestamp()
                return max(0, suspend_until - now) if suspend_until > now else None

        now = await self._current_timestamp()
        suspend_until = None
        if self.redis:
            raw = await self.redis.get(self._get_suspended_key(identifier))
            if raw:
                value = int(raw)
                if value > now:
                    suspend_until = value
        else:
            value = self._local_suspended.get(identifier)
            if value and value > now:
                suspend_until = value

        self._suspend_cache[identifier] = (time.time(), suspend_until)
        if suspend_until is None:
            return None
        return max(0, suspend_until - now)

    async def check_and_update(self, identifier: str) -> int:
        async with self._lock:
            suspended_wait = await self._check_suspended(identifier)
            if suspended_wait is not None and suspended_wait > 0:
                return suspended_wait

            config = await self._get_config(identifier)
            min_interval = int(config.window_size_millis / max(config.max_requests_per_second, 1e-6))
            ttl_seconds = max(1, int(config.window_size_millis / 1000))
            now = await self._current_timestamp()

            if self.redis:
                key = self._get_last_request_key(identifier)
                last = await self.redis.get(key)
                if last is not None:
                    last_ms = int(last)
                    diff = now - last_ms
                    if diff < min_interval:
                        return min_interval - diff
                await self.redis.set(key, now, ex=ttl_seconds)
                return 0

            last_ms = self._local_last_request.get(identifier)
            if last_ms is not None:
                diff = now - last_ms
                if diff < min_interval:
                    return min_interval - diff
            self._local_last_request[identifier] = now
            return 0

    async def wait_for_token(self, identifier: str, timeout_seconds: float = 10.0) -> bool:
        start = time.time()
        while True:
            wait_ms = await self.check_and_update(identifier)
            if wait_ms <= 0:
                return True
            elapsed = time.time() - start
            if elapsed >= timeout_seconds:
                return False
            await asyncio.sleep(min(wait_ms / 1000.0, timeout_seconds - elapsed))
