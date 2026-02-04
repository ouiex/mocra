import os
import sys
import pytest
import httpx

# ensure project root on path for imports
sys.path.append(os.getcwd())

from cacheable.service import CacheService, LocalBackend
from downloader.client import RequestDownloader
from common.models.request import Request


class DummyLimiter:
    async def wait_for_token(self, *args, **kwargs):
        return True


class DummyLockManager:
    async def acquire_lock(self, *args, **kwargs):
        return True

    async def release_lock(self, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_request_downloader_with_mock_transport():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"OK", headers={"X-Test": "1"})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)

    downloader = RequestDownloader(
        rate_limiter=DummyLimiter(),
        lock_manager=DummyLockManager(),
        cache_service=CacheService(LocalBackend(), namespace="test"),
    )
    downloader.enable_rate_limit = False
    downloader.default_client = client

    async def _get_client(_proxy):
        return client

    downloader._get_client = _get_client  # type: ignore[method-assign]

    req = Request(url="https://example.com/test", method="GET")
    resp = await downloader.download(req)

    assert resp.status_code == 200
    assert resp.content == b"OK"

    await client.aclose()
