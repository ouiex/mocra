import os
import sys
import pytest
import asyncio

# ensure project root on path for imports
sys.path.append(os.getcwd())

from sync.redis_backend import RedisCoordinationBackend
from fakeredis.aioredis import FakeRedis

@pytest.mark.asyncio
async def test_redis_coordination_basic():
    backend = RedisCoordinationBackend("redis://localhost:6379/0")
    # inject fake redis for testing
    backend._redis = FakeRedis(decode_responses=False)

    # set/get
    ok = await backend.set("test:key", b"value")
    assert ok is True or ok is None
    val = await backend.get("test:key")
    assert val == b"value"

    # acquire lock
    got = await backend.acquire_lock("test:lock", b"tok1", 5000)
    assert got is True
    # second acquire should fail
    got2 = await backend.acquire_lock("test:lock", b"tok2", 5000)
    assert got2 is False

    # renew lock (with correct token)
    renewed = await backend.renew_lock("test:lock", b"tok1", 6000)
    assert renewed is True

    # CAS
    await backend.set("test:cas", b"old")
    cas_ok = await backend.cas("test:cas", b"old", b"new")
    assert cas_ok is True
    cur = await backend.get("test:cas")
    assert cur == b"new"

    # CAS mismatch
    cas_fail = await backend.cas("test:cas", b"wrong", b"x")
    assert cas_fail is False

    # cleanup
    await backend._redis.flushall()

