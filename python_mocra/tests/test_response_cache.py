import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from uuid import UUID

from cacheable.service import CacheService, LocalBackend
from common.models.response import Response
from common.models.cookies import Cookies
from common.models.meta import MetaData
from common.models.context import ExecutionMark


@pytest.mark.asyncio
async def test_response_cache_roundtrip():
    cache = CacheService(LocalBackend(), namespace="test")

    resp = Response(
        id=UUID(int=1),
        platform="plat",
        account="acc",
        module="mod",
        status_code=200,
        cookies=Cookies(),
        content=b"hello",
        headers=[("Content-Type", "text/plain")],
        task_retry_times=0,
        metadata=MetaData(),
        download_middleware=[],
        data_middleware=[],
        task_finished=False,
        context=ExecutionMark(),
        run_id=UUID(int=2),
        prefix_request=UUID(int=3),
    )

    await cache.save_response("hash1", resp)
    loaded = await cache.get_response("hash1")

    assert loaded is not None
    assert loaded.status_code == 200
    assert loaded.content == b"hello"
    assert loaded.platform == "plat"
