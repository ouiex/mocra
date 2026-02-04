"""Proxy pool and statistics (port of Rust proxy::proxy_pool)."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .manager import Proxy


@dataclass
class PoolConfig:
    max_errors: int = 3


@dataclass
class ProxyStats:
    success_count: int = 0
    failure_count: int = 0
    error_streak: int = 0
    last_response_time_ms: Optional[int] = None
    last_used_ts: Optional[float] = None

    def record_success(self, response_time_ms: Optional[int] = None) -> None:
        self.success_count += 1
        self.error_streak = 0
        if response_time_ms is not None:
            self.last_response_time_ms = response_time_ms

    def record_failure(self) -> None:
        self.failure_count += 1
        self.error_streak += 1


@dataclass
class ProxyItem:
    proxy: "Proxy"
    stats: ProxyStats = field(default_factory=ProxyStats)

    def is_valid(self, max_errors: int) -> bool:
        return self.stats.error_streak < max_errors


class ProxyPool:
    def __init__(self, config: Optional[PoolConfig] = None) -> None:
        self.config = config or PoolConfig()
        self.pools: Dict[str, List[ProxyItem]] = {}
        self._lock = asyncio.Lock()
        self._rr_index = 0

    async def update_provider(self, provider_name: str, proxies: List["Proxy"]) -> None:
        items = [ProxyItem(proxy=p) for p in proxies]
        async with self._lock:
            self.pools[provider_name] = items

    async def get_proxy(self, provider_name: Optional[str] = None, strategy: str = "random") -> Optional["Proxy"]:
        async with self._lock:
            if provider_name:
                pool = self.pools.get(provider_name, [])
            else:
                pool = [item for items in self.pools.values() for item in items]

            pool = [item for item in pool if item.is_valid(self.config.max_errors)]
            if not pool:
                return None

            if strategy == "round_robin":
                proxy_item = pool[self._rr_index % len(pool)]
                self._rr_index += 1
                return proxy_item.proxy

            proxy_item = random.choice(pool)
            return proxy_item.proxy

    async def report_proxy_result(
        self,
        proxy: "Proxy",
        success: bool,
        response_time_ms: Optional[int] = None,
    ) -> None:
        async with self._lock:
            for items in self.pools.values():
                for item in items:
                    if item.proxy.url == proxy.url and item.proxy.provider == proxy.provider:
                        if success:
                            item.stats.record_success(response_time_ms)
                        else:
                            item.stats.record_failure()
                        break

            for provider_name, items in list(self.pools.items()):
                self.pools[provider_name] = [
                    item for item in items if item.is_valid(self.config.max_errors)
                ]

    async def report_success(self, proxy: "Proxy", response_time_ms: Optional[int] = None) -> None:
        await self.report_proxy_result(proxy, True, response_time_ms)

    async def report_failure(self, proxy: "Proxy") -> None:
        await self.report_proxy_result(proxy, False, None)

    async def get_pool_status(self) -> Dict[str, int]:
        async with self._lock:
            return {name: len(items) for name, items in self.pools.items()}
