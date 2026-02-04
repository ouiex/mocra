from typing import List, Optional
import logging
from redis.asyncio import Redis, RedisError

logger = logging.getLogger(__name__)

class Deduplicator:
    def __init__(self, redis_client: Redis, ttl: int = 86400):
        self.redis = redis_client
        self.ttl = ttl

    async def check_and_set(self, hash_key: str) -> bool:
        """
        Returns True if the item is new (set successfully), False if it existed.
        """
        key = f"dedup:{hash_key}"
        try:
            # set(nx=True) returns True if set happened, None/False if not
            result = await self.redis.set(key, "1", ex=self.ttl, nx=True)
            return bool(result)
        except RedisError as e:
            logger.error(f"Redis error in Deduplicator: {e}")
            # Fail open or closed? If Redis fails, we probably shouldn't dedupe incorrectly, 
            # effectively treating it as "new" might cause loops, treating as "cached" might lose data.
            # Rust implementation returns Error.
            raise e

    async def check_and_set_batch(self, hashes: List[str]) -> List[bool]:
        if not hashes:
            return []
            
        try:
            pipeline = self.redis.pipeline()
            for h in hashes:
                key = f"dedup:{h}"
                pipeline.set(key, "1", ex=self.ttl, nx=True)
            
            results = await pipeline.execute()
            # Redis-py pipeline set(nx=True) returns True if set, None if not set.
            return [bool(r) for r in results]
        except RedisError as e:
            logger.error(f"Redis error in Deduplicator batch: {e}")
            raise e
