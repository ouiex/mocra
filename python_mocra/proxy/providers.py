import logging
from typing import List, Optional

import httpx

from .manager import Proxy, ProxyProvider

logger = logging.getLogger(__name__)


class TextListProxyProvider(ProxyProvider):
    """Fetch proxies from a text list URL (IP:PORT[:USER:PASS])."""

    def __init__(
        self,
        name: str,
        url: str,
        timeout: int = 10,
        retry_codes: Optional[List[int]] = None,
        weight: Optional[int] = None,
    ):
        self.name = name
        self.url = url
        self.timeout = timeout
        self.retry_codes = retry_codes or []
        self.weight = weight or 1

    async def get_proxies(self) -> List[Proxy]:
        logger.info("Fetching proxies from %s: %s", self.name, self.url)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(self.url)
            resp.raise_for_status()
            text = resp.text

        proxies: List[Proxy] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(":")
            if len(parts) < 2:
                continue
            host = parts[0].strip()
            port = parts[1].strip()
            if not host or not port:
                continue
            url = f"http://{host}:{port}"
            username = parts[2].strip() if len(parts) >= 4 else None
            password = parts[3].strip() if len(parts) >= 4 else None
            proxies.append(Proxy(url=url, username=username, password=password, provider=self.name))

        logger.info("Fetched %d proxies from %s", len(proxies), self.name)
        return proxies
