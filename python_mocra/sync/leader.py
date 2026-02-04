import asyncio
import logging
import uuid
from typing import Optional
from redis.asyncio import Redis, RedisError

logger = logging.getLogger(__name__)

class LeaderElector:
    def __init__(self, redis_client: Optional[Redis], key: str, ttl_ms: int = 5000):
        self.redis = redis_client
        self.key = key
        self.id = str(uuid.uuid4())
        self.ttl_ms = ttl_ms
        self._is_leader = False
        self._running = False
        self._task = None
        
        self._renew_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def start(self):
        if self._running:
            return
        self._running = True
        
        if not self.redis:
            logger.info("LeaderElector: No Redis backend, enabling local mode (always leader)")
            self._is_leader = True
            return

        self._task = asyncio.create_task(self._election_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _election_loop(self):
        logger.info(f"LeaderElector[{self.id}] started election loop for {self.key}")
        
        while self._running:
            try:
                if not self._is_leader:
                    acquired = await self.redis.set(
                        self.key, 
                        self.id, 
                        nx=True, 
                        px=self.ttl_ms
                    )
                    
                    if acquired:
                        self._is_leader = True
                        logger.info(f"LeaderElector[{self.id}]: Acquired leadership")
                    else:
                        await asyncio.sleep(self.ttl_ms / 1000.0)
                        
                if self._is_leader:
                    renew_interval = (self.ttl_ms / 3) / 1000.0
                    await asyncio.sleep(renew_interval)
                    
                    renewed = await self.redis.eval(
                        self._renew_script, 
                        1, 
                        self.key, 
                        self.id, 
                        self.ttl_ms
                    )
                    
                    if not renewed:
                        logger.warning(f"LeaderElector[{self.id}]: Lost leadership during renewal")
                        self._is_leader = False

            except asyncio.CancelledError:
                break
            except RedisError as e:
                logger.error(f"LeaderElector redis error: {e}")
                self._is_leader = False
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"LeaderElector unexpected error: {e}")
                self._is_leader = False
                await asyncio.sleep(1.0)
