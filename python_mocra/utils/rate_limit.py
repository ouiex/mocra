from abc import ABC, abstractmethod
from typing import Dict, Deque
from collections import defaultdict, deque
import time
import asyncio
import logging

logger = logging.getLogger(__name__)

class BaseRateLimiter(ABC):
    """
    Abstract Base Class for Rate Limiters.
    """
    @abstractmethod
    async def acquire(self, key: str, limit: int = 1, window: float = 1.0) -> bool:
        pass
    
    @abstractmethod
    async def wait_for_token(self, key: str, limit: int = 1, window: float = 1.0, timeout: float = 10.0) -> bool:
        pass

class MemoryRateLimiter(BaseRateLimiter):
    """
    In-memory Sliding Window Rate Limiter.
    """
    def __init__(self):
        self.windows: Dict[str, Deque[float]] = defaultdict(deque)

    async def acquire(self, key: str, limit: int = 1, window: float = 1.0) -> bool:
        now = time.time()
        timestamps = self.windows[key]
        
        while timestamps and timestamps[0] < now - window:
            timestamps.popleft()
            
        if len(timestamps) < limit:
            timestamps.append(now)
            return True
        return False

    async def wait_for_token(self, key: str, limit: int = 1, window: float = 1.0, timeout: float = 10.0) -> bool:
        start_wait = time.time()
        while True:
            if await self.acquire(key, limit, window):
                return True
            
            elapsed = time.time() - start_wait
            if elapsed > timeout:
                return False
                
            timestamps = self.windows[key]
            if timestamps:
                oldest = timestamps[0]
                wait_time = (oldest + window) - time.time()
                if wait_time < 0:
                    wait_time = 0.01
            else:
                wait_time = 0.1 
                
            if elapsed + wait_time > timeout:
                wait_time = timeout - elapsed
                
            if wait_time > 0:
                await asyncio.sleep(wait_time)


class RedisSlidingWindowLimiter(BaseRateLimiter):
    """
    Redis-based Sliding Window Rate Limiter using Lua script.
    """
    LUA_SCRIPT = """
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])
    local member_id = ARGV[4]
    
    redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
    local count = redis.call('ZCARD', key)
    
    if count < limit then
        redis.call('ZADD', key, now, member_id)
        redis.call('EXPIRE', key, window + 1)
        return 1
    else
        return 0
    end
    """

    def __init__(self, redis_client):
        self.redis = redis_client
        self.script_sha = None

    async def _ensure_script(self):
        if not self.script_sha:
            self.script_sha = await self.redis.script_load(self.LUA_SCRIPT)

    async def acquire(self, key: str, limit: int = 1, window: float = 1.0) -> bool:
        await self._ensure_script()
        now = time.time()
        import uuid
        member_id = f"{now}-{uuid.uuid4()}"
        
        result = await self.redis.evalsha(
            self.script_sha, 1, key, limit, window, now, member_id
        )
        return bool(result)

    async def wait_for_token(self, key: str, limit: int = 1, window: float = 1.0, timeout: float = 10.0) -> bool:
        start_wait = time.time()
        while True:
            if await self.acquire(key, limit, window):
                return True
            
            elapsed = time.time() - start_wait
            if elapsed > timeout:
                return False
                
            await asyncio.sleep(0.1) 
