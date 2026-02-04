import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from pydantic import BaseModel

from .pool import PoolConfig, ProxyPool

logger = logging.getLogger(__name__)

class Proxy(BaseModel):
    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    provider: Optional[str] = None
    
    @property
    def connection_string(self) -> str:
        """Returns the full connection string with auth if provided in fields but not url."""
        # This is a simplification. Usually url contains everything.
        return self.url

class ProxyProvider(ABC):
    @abstractmethod
    async def get_proxies(self) -> List[Proxy]:
        pass

class ManualProxyProvider(ProxyProvider):
    def __init__(self, proxies: List[str]):
        self.proxies = [Proxy(url=p) for p in proxies]

    async def get_proxies(self) -> List[Proxy]:
        return self.proxies

class ProxyManager:
    def __init__(self, config: Optional[PoolConfig] = None):
        self.providers: List[ProxyProvider] = []
        self.pool = ProxyPool(config)
        self._lock = asyncio.Lock()
        self._running = False
        self._refresh_task = None

    def register_provider(self, provider: ProxyProvider):
        self.providers.append(provider)

    async def start(self, refresh_interval: int = 300):
        self._running = True
        # Initial refresh
        await self.refresh_proxies()
        self._refresh_task = asyncio.create_task(self._loop(refresh_interval))

    async def stop(self):
        self._running = False
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass

    async def _loop(self, interval: int):
        while self._running:
            await asyncio.sleep(interval)
            await self.refresh_proxies()

    async def refresh_proxies(self):
        total = 0
        for provider in self.providers:
            name = getattr(provider, "name", provider.__class__.__name__)
            try:
                proxies = await provider.get_proxies()
                for proxy in proxies:
                    if not proxy.provider:
                        proxy.provider = name
                await self.pool.update_provider(name, proxies)
                total += len(proxies)
            except Exception as e:
                logger.error("Error fetching proxies from provider %s: %s", name, e)

        logger.info("Refreshed proxy pool: %d proxies", total)

    async def get_proxy(self, strategy: str = "default", provider_name: Optional[str] = None) -> Optional[Proxy]:
        strategy = "round_robin" if strategy == "round_robin" else "random"
        return await self.pool.get_proxy(provider_name=provider_name, strategy=strategy)

    async def report_proxy_result(
        self,
        proxy: Proxy,
        success: bool,
        response_time_ms: Optional[int] = None,
    ) -> None:
        await self.pool.report_proxy_result(proxy, success, response_time_ms)

    async def report_success(self, proxy: Proxy, response_time_ms: Optional[int] = None) -> None:
        await self.pool.report_success(proxy, response_time_ms)

    async def report_failure(self, proxy: Proxy) -> None:
        await self.pool.report_failure(proxy)

    async def get_status(self):
        return await self.pool.get_pool_status()
