import os
import sys
import pytest
import asyncio

# ensure project root on path for imports
sys.path.append(os.getcwd())

from fakeredis.aioredis import FakeRedis

from utils.rate_limit import MemoryRateLimiter, RedisSlidingWindowLimiter


@pytest.mark.asyncio
async def test_memory_rate_limiter_basic():
    limiter = MemoryRateLimiter()
    key = "k"

    assert await limiter.acquire(key, limit=2, window=1.0) is True
    assert await limiter.acquire(key, limit=2, window=1.0) is True
    assert await limiter.acquire(key, limit=2, window=1.0) is False

    await asyncio.sleep(1.05)
    assert await limiter.acquire(key, limit=2, window=1.0) is True


@pytest.mark.asyncio
async def test_redis_rate_limiter_basic():
    redis = FakeRedis(decode_responses=True)
    limiter = RedisSlidingWindowLimiter(redis)
    key = "rate:key"

    ok1 = await limiter.acquire(key, limit=2, window=1.0)
    ok2 = await limiter.acquire(key, limit=2, window=1.0)
    ok3 = await limiter.acquire(key, limit=2, window=1.0)

    assert ok1 is True
    assert ok2 is True
    assert ok3 is False

    await asyncio.sleep(1.05)
    ok4 = await limiter.acquire(key, limit=2, window=1.0)
    assert ok4 is True
