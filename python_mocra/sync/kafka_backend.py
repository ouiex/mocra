"""
KafkaCoordinationBackend

Provides publish/subscribe via Kafka. For key-value, locks and CAS operations it delegates
to an optional RedisCoordinationBackend when provided. If no Redis backend is available,
KV/lock/CAS operations raise NotImplementedError because Kafka alone does not provide
strongly consistent KV or locking semantics.

This pragmatic approach allows using Kafka for pub/sub while relying on Redis for
coordination operations (matching many real deployments).
"""
from typing import Optional, AsyncIterator, Dict
import asyncio
import logging
import uuid

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from .backend import CoordinationBackend
from .redis_backend import RedisCoordinationBackend

logger = logging.getLogger(__name__)

class KafkaCoordinationBackend(CoordinationBackend):
    def __init__(self, bootstrap_servers: Optional[str] = None, redis_backend: Optional[RedisCoordinationBackend] = None):
        self.bootstrap_servers = bootstrap_servers
        self.redis_backend = redis_backend
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumers: Dict[str, AIOKafkaConsumer] = {}
        # Note: actual Kafka producer/consumer initialization is lazy and
        # not required for unit tests that only exercise Redis delegation.

    async def close(self):
        # Close underlying redis backend if present
        if self.redis_backend:
            await self.redis_backend.close()
        if self._producer:
            await self._producer.stop()
            self._producer = None
        for consumer in self._consumers.values():
            await consumer.stop()
        self._consumers.clear()

    async def get(self, key: str) -> Optional[bytes]:
        if self.redis_backend:
            return await self.redis_backend.get(key)
        raise NotImplementedError("KafkaCoordinationBackend requires Redis for get/set operations")

    async def set(self, key: str, value: bytes, ttl_ms: Optional[int] = None) -> bool:
        if self.redis_backend:
            return await self.redis_backend.set(key, value, ttl_ms)
        raise NotImplementedError("KafkaCoordinationBackend requires Redis for get/set operations")

    async def publish(self, topic: str, message: bytes) -> bool:
        if not self.bootstrap_servers:
            logger.debug("publish: no bootstrap_servers configured, skipping Kafka publish")
            return False
        if not self._producer:
            self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
            await self._producer.start()
        try:
            await self._producer.send_and_wait(topic, message)
            return True
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")
            return False

    async def subscribe(self, topic: str) -> AsyncIterator[bytes]:
        if not self.bootstrap_servers:
            raise NotImplementedError("Kafka subscribe not available when bootstrap_servers is not configured")

        group_id = f"sync_group_{uuid.uuid4().hex}"
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            client_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        await consumer.start()
        self._consumers[group_id] = consumer

        try:
            while True:
                try:
                    msg = await consumer.getone()
                    if msg and msg.value is not None:
                        yield msg.value
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Kafka subscribe error on {topic}: {e}")
                    await asyncio.sleep(1)
        finally:
            await consumer.stop()
            self._consumers.pop(group_id, None)

    async def acquire_lock(self, key: str, value: bytes, ttl_ms: int) -> bool:
        if self.redis_backend:
            return await self.redis_backend.acquire_lock(key, value, ttl_ms)
        raise NotImplementedError("KafkaCoordinationBackend requires Redis for lock operations")

    async def renew_lock(self, key: str, value: bytes, ttl_ms: int) -> bool:
        if self.redis_backend:
            return await self.redis_backend.renew_lock(key, value, ttl_ms)
        raise NotImplementedError("KafkaCoordinationBackend requires Redis for lock operations")

    async def cas(self, key: str, old_value: Optional[bytes], new_value: bytes) -> bool:
        if self.redis_backend:
            return await self.redis_backend.cas(key, old_value, new_value)
        raise NotImplementedError("KafkaCoordinationBackend requires Redis for CAS operations")
