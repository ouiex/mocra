import asyncio
import pytest
import time
from redis.asyncio import Redis
from sync.lock import DistributedLock, DistributedSemaphore

# Mock Redis for testing or use real one if available?
# For distributed lock, Mocking redis behavior (SET NX PX) is tricky but doable.
# Better to assume user has Redis or skip if not.
# We will use fakeredis if installed, or skip.

try:
    from fakeredis.aioredis import FakeRedis
    has_fakeredis = True
except ImportError:
    has_fakeredis = False

@pytest.mark.asyncio
async def test_distributed_lock():
    if not has_fakeredis:
        pytest.skip("fakeredis not installed")
        
    redis = FakeRedis(decode_responses=True)
    
    lock1 = DistributedLock(redis, "resource_a", ttl_ms=2000, retry_interval=0.01)
    lock2 = DistributedLock(redis, "resource_a", ttl_ms=2000, retry_interval=0.01)
    
    # 1. Acquire Lock 1
    assert await lock1.acquire() is True
    
    # 2. Try Acquire Lock 2 (Should Fail)
    assert await lock2.acquire() is False
    
    # 3. Release Lock 1
    await lock1.release()
    
    # 4. Acquire Lock 2 (Should Success)
    assert await lock2.acquire() is True
    
    # 5. Context Manager
    await lock2.release()
    async with DistributedLock(redis, "resource_a") as lock:
        assert lock._held is True
        # Try another
        assert await lock1.acquire() is False
        
    # Should be released now
    assert await lock1.acquire() is True

@pytest.mark.asyncio
async def test_distributed_semaphore():
    if not has_fakeredis:
        pytest.skip("fakeredis not installed")

    redis = FakeRedis(decode_responses=True)
    sem = DistributedSemaphore(redis, "limit_b", limit=2, ttl_ms=5000)
    
    # 1. Acquire 2 slots
    assert await sem.acquire() is True
    assert await sem.acquire() is True
    
    # 2. Acquire 3rd (Should Fail)
    assert await sem.acquire(timeout=0.2) is False
    
    # 3. Release one
    await sem.release() # Releases the last acquired token by this instance object? 
    # NOTE: Semaphore implementation usually uses unique token per acquire if re-entrant or object holds state.
    # My implementation uses `self.token = uuid`, so one generic object holds ONE slot. 
    # To test N slots, we need N objects.
    
    sem1 = DistributedSemaphore(redis, "limit_c", limit=2)
    sem2 = DistributedSemaphore(redis, "limit_c", limit=2)
    sem3 = DistributedSemaphore(redis, "limit_c", limit=2)
    
    assert await sem1.acquire() is True
    assert await sem2.acquire() is True
    assert await sem3.acquire(timeout=0.1) is False
    
    await sem1.release()
    assert await sem3.acquire() is True
