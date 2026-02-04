import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from fakeredis.aioredis import FakeRedis
from sync.kafka_backend import KafkaCoordinationBackend
from sync.redis_backend import RedisCoordinationBackend

@pytest.mark.asyncio
async def test_kafka_backend_delegates_to_redis():
    # Prepare fake redis and wrap in RedisCoordinationBackend
    redis_coord = RedisCoordinationBackend("redis://localhost:6379/0")
    redis_coord._redis = FakeRedis(decode_responses=False)

    kb = KafkaCoordinationBackend(bootstrap_servers=None, redis_backend=redis_coord)

    # set/get
    ok = await kb.set("k1", b"v1")
    assert ok is True or ok is None
    v = await kb.get("k1")
    assert v == b"v1"

    # CAS
    await kb.set("cas", b"old")
    cas_ok = await kb.cas("cas", b"old", b"new")
    assert cas_ok is True
    cur = await kb.get("cas")
    assert cur == b"new"

    # Locks
    got = await kb.acquire_lock("l1", b"tok", 5000)
    assert got is True
    got2 = await kb.acquire_lock("l1", b"tok2", 5000)
    assert got2 is False

    # Cleanup
    await redis_coord._redis.flushall()
