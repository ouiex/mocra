import asyncio
import uuid
from uuid6 import uuid7
import time
import logging
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

class DistributedLock:
    def __init__(self, redis_client: Redis, resource: str, ttl_ms: int = 10000, retry_interval: float = 0.1, max_retries: int = 3):
        self.redis = redis_client
        self.resource = f"lock:{resource}"
        self.token = str(uuid.uuid4())
        self.ttl_ms = ttl_ms
        self.retry_delay = retry_interval
        self.max_retries = max_retries
        self._held = False
        
        self._release_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """

    async def acquire(self) -> bool:
        if self._held:
            return True
            
        for _ in range(self.max_retries + 1):
            acquired = await self.redis.set(
                self.resource, 
                self.token, 
                nx=True, 
                px=self.ttl_ms
            )
            if acquired:
                self._held = True
                return True
            
            await asyncio.sleep(self.retry_delay)
            
        return False

    async def release(self):
        if not self._held:
            return
            
        try:
            # Fallback to python-side check if eval is not supported (e.g. fakeredis issues)
            # await self.redis.eval(self._release_script, 1, self.resource, self.token)
            val = await self.redis.get(self.resource)
            if val == self.token:
                await self.redis.delete(self.resource)
        except Exception as e:
            logger.error(f"Error releasing lock {self.resource}: {e}")
        finally:
            self._held = False

    async def __aenter__(self):
        if await self.acquire():
            return self
        raise Exception(f"Failed to acquire lock for {self.resource}")

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()


class DistributedSemaphore:
    def __init__(self, redis_client: Redis, resource: str, limit: int = 1, ttl_ms: int = 10000):
        self.redis = redis_client
        self.resource = f"sem:{resource}"
        self.limit = limit
        self.ttl_ms = ttl_ms
        self.tokens = []

    async def acquire(self, timeout: float = 0.0) -> bool:
        token = str(uuid7())
        start_time = time.time()
        while True:
            # 1. Clean expired entries
            now = time.time()
            cutoff = now - (self.ttl_ms / 1000.0)
            await self.redis.zremrangebyscore(self.resource, "-inf", cutoff)
            
            # 2. Try to add ourselves
            # Use now as score
            await self.redis.zadd(self.resource, {token: now})
            
            # 3. Check rank
            rank = await self.redis.zrank(self.resource, token)
            
            if rank is not None and rank < self.limit:
                # We are in!
                # Set expiry on the key itself to auto-cleanup if everything dies
                await self.redis.expire(self.resource, int(self.ttl_ms / 1000) + 10)
                self.tokens.append(token)
                return True
            else:
                # Failed, remove ourselves
                await self.redis.zrem(self.resource, token)
                
                if timeout <= 0 or (time.time() - start_time) >= timeout:
                    return False
                
                await asyncio.sleep(0.1)

    async def release(self):
        if self.tokens:
            token = self.tokens.pop()
            await self.redis.zrem(self.resource, token)

    async def __aenter__(self):
        if await self.acquire():
            return self
        raise Exception(f"Failed to acquire semaphore for {self.resource}")

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()
