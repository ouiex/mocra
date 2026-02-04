import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from cacheable.service import CacheService, LocalBackend
from common.models.headers import Headers
from common.models.cookies import Cookies


@pytest.mark.asyncio
async def test_cache_headers_and_cookies_roundtrip():
    cache = CacheService(LocalBackend(), namespace="test")

    headers = Headers()
    headers.add("User-Agent", "TestAgent")
    headers.add("X-Test", "1")

    cookies = Cookies()
    cookies.add("sid", "abc", "example.com")

    await cache.save_headers("mod1", headers)
    await cache.save_cookies("mod1", cookies)

    loaded_headers = await cache.get_headers("mod1")
    loaded_cookies = await cache.get_cookies("mod1")

    assert loaded_headers is not None
    assert loaded_headers.get("User-Agent") == "TestAgent"
    assert loaded_headers.get("X-Test") == "1"

    assert loaded_cookies is not None
    assert loaded_cookies.contains("sid", "example.com")
