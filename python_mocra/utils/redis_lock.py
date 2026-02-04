import asyncio
import logging
import time
import uuid
from typing import Optional
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

class DistributedLock:
    """
    A distributed lock implementation using Redis.
    """
    def __init__(self, redis_client: Redis, key: str, ttl_ms: int = 10000, retry_interval: float = 0.1, retries: int = 20):
        self.redis = redis_client
        self.key = f"lock:{key}"
        self.token = str(uuid.uuid4())
        self.ttl_ms = ttl_ms
        self.retry_interval = retry_interval
        self.retries = retries
        self._held = False

    async def acquire(self) -> bool:
        for _ in range(self.retries):
            if await self.redis.set(self.key, self.token, nx=True, px=self.ttl_ms):
                self._held = True
                return True
            await asyncio.sleep(self.retry_interval)
        return False

    async def release(self):
        if not self._held:
            return

        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            await self.redis.eval(script, 1, self.key, self.token)
        except Exception as e:
            logger.error(f"Error releasing lock {self.key}: {e}")
        finally:
            self._held = False

    async def __aenter__(self):
        if not await self.acquire():
            raise TimeoutError(f"Could not acquire lock {self.key}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

class DistributedSemaphore:
    """
    A distributed semaphore implementation using Redis ZSET.
    """
    def __init__(self, redis_client: Redis, key: str, limit: int, ttl_ms: int = 60000):
        self.redis = redis_client
        self.key = f"sem:{key}"
        self.limit = limit
        self.ttl_ms = ttl_ms
        self.token = str(uuid.uuid4())
        self._held = False

    async def acquire(self, timeout: float = 5.0) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            now = time.time()
            now_ms = int(now * 1000)
            expire_ms = now_ms - self.ttl_ms
            
            async with self.redis.pipeline(transaction=True) as pipe:
                try:
                    pipe.zremrangebyscore(self.key, "-inf", expire_ms)
                    pipe.zcard(self.key)
                    results = await pipe.execute()
                    count = results[1]
                    
                    if count < self.limit:
                        added = await self.redis.zadd(self.key, {self.token: now_ms})
                        if added:
                            self._held = True
                            return True
                except Exception:
                    pass
            await asyncio.sleep(0.1)
        return False

    async def release(self):
        if self._held:
            try:
                await self.redis.zrem(self.key, self.token)
            except Exception as e:
                logger.error(f"Error releasing semaphore {self.key}: {e}")
            self._held = False
            
    async def __aenter__(self):
        if not await self.acquire():
             raise TimeoutError(f"Could not acquire semaphore {self.key}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

class DistributedLockManager:
    def __init__(self, redis: Redis, namespace: str = "lock"):
        self.redis = redis
        self.namespace = namespace

    def _key(self, resource_id: str) -> str:
        return f"{self.namespace}:{resource_id}"

    async def acquire_lock(self, resource_id: str, ttl_sec: int = 30, timeout_sec: float = 10.0) -> bool:
        key = self._key(resource_id)
        token = str(uuid.uuid4())
        
        start_time = asyncio.get_event_loop().time()
        while True:
            # Set lock if not exists
            if await self.redis.set(key, token, ex=ttl_sec, nx=True):
                return True
            
            if asyncio.get_event_loop().time() - start_time > timeout_sec:
                return False
            
            await asyncio.sleep(0.1)

    async def release_lock(self, resource_id: str) -> None:
        key = self._key(resource_id)
        await self.redis.delete(key)
