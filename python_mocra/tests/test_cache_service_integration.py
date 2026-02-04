import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from cacheable.service import CacheService, LocalBackend
from pydantic import BaseModel

class SimpleModel(BaseModel):
    x: int

@pytest.mark.asyncio
async def test_cacheservice_set_get_json():
    backend = LocalBackend()
    svc = CacheService(backend, namespace="testsrv")

    model = SimpleModel(x=42)
    await svc.set_json("cfg", model, ttl=60)

    loaded = await svc.get_json("cfg", SimpleModel)
    assert loaded is not None
    assert loaded.x == 42

    # raw set/get
    await svc.set("raw", b"bytes", ttl=10)
    assert await svc.get("raw") == b"bytes"

