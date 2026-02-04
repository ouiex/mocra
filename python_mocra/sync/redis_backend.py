"""
Redis implementation of CoordinationBackend
"""
import asyncio
import logging
from typing import Optional, AsyncIterator
from redis.asyncio import Redis, ConnectionPool
from .backend import CoordinationBackend

logger = logging.getLogger(__name__)

class RedisCoordinationBackend(CoordinationBackend):
    """Redis-based coordination backend"""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._pool = ConnectionPool.from_url(redis_url, decode_responses=False)
        self._redis: Redis = Redis(connection_pool=self._pool)
        self._pubsub_clients = {}
        
    async def close(self):
        """Cleanup connections"""
        for client in self._pubsub_clients.values():
            await client.close()
        self._pubsub_clients.clear()
        await self._redis.close()
        await self._pool.disconnect()
    
    async def get(self, key: str) -> Optional[bytes]:
        """Get value for key"""
        value = await self._redis.get(key)
        return value if value is not None else None
    
    async def set(self, key: str, value: bytes, ttl_ms: Optional[int] = None) -> bool:
        """Set key-value pair with optional TTL"""
        if ttl_ms:
            return await self._redis.set(key, value, px=ttl_ms)
        else:
            return await self._redis.set(key, value)
    
    async def publish(self, topic: str, message: bytes) -> bool:
        """Publish message to Redis Stream"""
        try:
            await self._redis.xadd(topic, {"payload": message})
            return True
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")
            return False
    
    async def subscribe(self, topic: str) -> AsyncIterator[bytes]:
        """Subscribe to Redis Stream and yield messages"""
        # Create consumer group if not exists
        try:
            await self._redis.xgroup_create(topic, "sync_group", id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.warning(f"Error creating group for {topic}: {e}")
        
        consumer_id = f"consumer_{id(self)}"
        
        while True:
            try:
                # Read from stream
                messages = await self._redis.xreadgroup(
                    "sync_group",
                    consumer_id,
                    {topic: ">"},
                    count=1,
                    block=2000
                )
                
                if messages:
                    _, msg_list = messages[0]
                    for msg_id, fields in msg_list:
                        if "payload" in fields:
                            # ACK the message
                            await self._redis.xack(topic, "sync_group", msg_id)
                            yield fields["payload"]
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in subscribe loop for {topic}: {e}")
                await asyncio.sleep(1)
    
    async def acquire_lock(self, key: str, value: bytes, ttl_ms: int) -> bool:
        """Try to acquire distributed lock using SET NX"""
        result = await self._redis.set(key, value, nx=True, px=ttl_ms)
        return bool(result)
    
    async def renew_lock(self, key: str, value: bytes, ttl_ms: int) -> bool:
        """Renew lock TTL if we own it"""
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        try:
            result = await self._redis.eval(script, 1, key, value, ttl_ms)
            return bool(result)
        except Exception as e:
            logger.error(f"Error renewing lock {key}: {e}")
            return False
    
    async def cas(self, key: str, old_value: Optional[bytes], new_value: bytes) -> bool:
        """Compare-and-swap operation"""
        # Use WATCH + MULTI + EXEC for CAS
        async with self._redis.pipeline() as pipe:
            try:
                await pipe.watch(key)
                current = await pipe.get(key)
                
                # Check if current value matches expected old_value
                if current != old_value:
                    await pipe.unwatch()
                    return False
                
                # Execute the swap
                pipe.multi()
                await pipe.set(key, new_value)
                await pipe.execute()
                return True
                
            except Exception as e:
                logger.error(f"CAS failed for {key}: {e}")
                return False
