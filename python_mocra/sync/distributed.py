"""
Distributed Synchronization Service - Python implementation of Rust's DistributedSync
"""
import asyncio
import logging
import pickle
import time
from typing import TypeVar, Generic, Optional, Type, Dict, Callable
from abc import ABC
from dataclasses import dataclass

from .backend import CoordinationBackend

logger = logging.getLogger(__name__)

T = TypeVar('T')

class SyncAble(ABC):
    """
    Trait for types that can be synchronized across distributed nodes.
    Similar to Rust's SyncAble trait.
    """
    
    @classmethod
    def topic(cls) -> str:
        """Return the topic name for this sync type"""
        raise NotImplementedError("Subclasses must implement topic()")
    
    def serialize(self) -> bytes:
        """Serialize to bytes (default uses pickle)"""
        return pickle.dumps(self)
    
    @classmethod
    def deserialize(cls: Type[T], data: bytes) -> T:
        """Deserialize from bytes"""
        return pickle.loads(data)


class DistributedSync(Generic[T]):
    """
    Distributed state synchronization.
    Watches for changes to shared state across nodes.
    """
    
    def __init__(self):
        self._value: Optional[T] = None
        self._changed_event = asyncio.Event()
        
    def get(self) -> Optional[T]:
        """Get current value"""
        return self._value
    
    def _update(self, value: Optional[T]):
        """Internal: update value and notify waiters"""
        self._value = value
        self._changed_event.set()
        self._changed_event.clear()
    
    async def changed(self) -> None:
        """Wait for value to change"""
        await self._changed_event.wait()


class SyncService:
    """
    Service for managing distributed state synchronization.
    Replicates Rust's SyncService functionality.
    """
    
    # Class-level store for local-only mode
    _local_store: Dict[str, Dict] = {}
    
    def __init__(self, backend: Optional[CoordinationBackend], namespace: str = ""):
        self.backend = backend
        self.namespace = namespace
        self._subscriptions: Dict[str, asyncio.Task] = {}
        
    def _stream_topic_for(self, topic: str) -> str:
        """Get stream topic name with namespace"""
        if self.namespace:
            return f"sync_stream_{self.namespace}_{topic}"
        return f"sync_stream_{topic}"
    
    def _kv_key_for(self, topic: str) -> str:
        """Get KV key with namespace"""
        if self.namespace:
            return f"sync_kv:{self.namespace}:{topic}"
        return f"sync_kv:{topic}"
    
    def _get_local_state(self, topic: str) -> Dict:
        """Get or create local state dict for topic"""
        key = f"sync:{self.namespace}:{topic}" if self.namespace else f"sync:{topic}"
        if key not in self._local_store:
            self._local_store[key] = {"value": None, "watchers": [], "lock": asyncio.Lock()}
        return self._local_store[key]
    
    async def sync(self, sync_type: Type[T]) -> DistributedSync[T]:
        """
        Create a distributed sync handle for the given type.
        
        In distributed mode: subscribes to changes via backend.
        In local mode: uses in-memory shared state.
        """
        if not issubclass(sync_type, SyncAble):
            raise TypeError(f"{sync_type} must inherit from SyncAble")
        
        topic = sync_type.topic()
        
        if self.backend:
            # Distributed mode
            return await self._sync_distributed(sync_type, topic)
        else:
            # Local mode
            return self._sync_local(sync_type, topic)
    
    async def _sync_distributed(self, sync_type: Type[T], topic: str) -> DistributedSync[T]:
        """Set up distributed synchronization"""
        stream_topic = self._stream_topic_for(topic)
        kv_key = self._kv_key_for(topic)
        
        # Create sync object
        sync_obj = DistributedSync[T]()
        
        # Load initial value from KV store
        initial_data = await self.backend.get(kv_key)
        if initial_data:
            try:
                initial_value = sync_type.deserialize(initial_data)
                sync_obj._update(initial_value)
            except Exception as e:
                logger.error(f"Failed to deserialize initial value for {topic}: {e}")
        
        # Start subscription task
        async def subscription_loop():
            try:
                async for message in self.backend.subscribe(stream_topic):
                    try:
                        value = sync_type.deserialize(message)
                        sync_obj._update(value)
                    except Exception as e:
                        logger.error(f"Failed to deserialize update for {topic}: {e}")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Subscription loop error for {topic}: {e}")
        
        # Also periodically refresh from KV in case pub/sub misses updates
        async def refresh_loop():
            try:
                while True:
                    await asyncio.sleep(30)
                    data = await self.backend.get(kv_key)
                    if data:
                        try:
                            value = sync_type.deserialize(data)
                            sync_obj._update(value)
                        except Exception as e:
                            logger.error(f"Failed to deserialize refresh for {topic}: {e}")
            except asyncio.CancelledError:
                pass
        
        # Launch tasks
        sub_task = asyncio.create_task(subscription_loop())
        refresh_task = asyncio.create_task(refresh_loop())
        
        # Store tasks for cleanup
        self._subscriptions[topic] = asyncio.gather(sub_task, refresh_task)
        
        return sync_obj

    async def _publish_with_retry(self, topic: str, payload: bytes, max_retries: int = 3) -> bool:
        if not self.backend:
            return False
        attempt = 0
        while True:
            ok = await self.backend.publish(topic, payload)
            if ok:
                return True
            attempt += 1
            if attempt > max_retries:
                return False
            backoff_ms = 50 * (2 ** min(attempt, 4))
            jitter_ms = int(time.time_ns() % 25)
            await asyncio.sleep((backoff_ms + jitter_ms) / 1000.0)
    
    def _sync_local(self, sync_type: Type[T], topic: str) -> DistributedSync[T]:
        """Set up local-only synchronization (single process)"""
        local_state = self._get_local_state(topic)
        sync_obj = DistributedSync[T]()
        
        # Load current value
        if local_state["value"] is not None:
            sync_obj._update(local_state["value"])
        
        # Register watcher
        local_state["watchers"].append(sync_obj)
        
        return sync_obj
    
    async def publish(self, value: SyncAble) -> bool:
        """
        Publish updated state to all subscribers.
        Updates both KV store and pub/sub stream.
        """
        topic = value.topic()
        data = value.serialize()
        
        if self.backend:
            # Distributed mode
            kv_key = self._kv_key_for(topic)
            stream_topic = self._stream_topic_for(topic)
            
            # Update KV store
            await self.backend.set(kv_key, data)
            
            # Publish to stream
            return await self._publish_with_retry(stream_topic, data)
        else:
            # Local mode
            local_state = self._get_local_state(topic)
            local_state["value"] = value
            
            # Notify all local watchers
            for watcher in local_state["watchers"]:
                watcher._update(value)
            
            return True

    async def send(self, value: SyncAble) -> bool:
        """Alias for publish to match Rust API"""
        return await self.publish(value)

    async def fetch_latest(self, sync_type: Type[T]) -> Optional[T]:
        """Fetch latest value from KV (distributed) or local store"""
        if not issubclass(sync_type, SyncAble):
            raise TypeError(f"{sync_type} must inherit from SyncAble")

        topic = sync_type.topic()
        if self.backend:
            kv_key = self._kv_key_for(topic)
            data = await self.backend.get(kv_key)
            if not data:
                return None
            try:
                return sync_type.deserialize(data)
            except Exception as e:
                logger.error(f"Failed to deserialize latest value for {topic}: {e}")
                return None
        else:
            local_state = self._get_local_state(topic)
            value = local_state["value"]
            return value

    async def optimistic_update(self, sync_type: Type[T], updater: Callable[[T], None]) -> T:
        """Optimistically update state using CAS in distributed mode or atomic lock in local mode"""
        if not issubclass(sync_type, SyncAble):
            raise TypeError(f"{sync_type} must inherit from SyncAble")

        topic = sync_type.topic()

        if self.backend:
            kv_key = self._kv_key_for(topic)
            stream_topic = self._stream_topic_for(topic)
            attempts = 0

            while True:
                old_bytes = await self.backend.get(kv_key)
                if old_bytes is None:
                    raise RuntimeError("Cannot update non-existent state")

                try:
                    state = sync_type.deserialize(old_bytes)
                except Exception as e:
                    raise RuntimeError(f"Failed to deserialize state for update: {e}") from e

                updater(state)
                new_bytes = state.serialize()

                if await self.backend.cas(kv_key, old_bytes, new_bytes):
                    await self._publish_with_retry(stream_topic, new_bytes)
                    return state

                attempts = min(attempts + 1, 6)
                backoff_ms = 10 * (2 ** min(attempts, 4))
                jitter_ms = int(time.time_ns() % 25)
                await asyncio.sleep((backoff_ms + jitter_ms) / 1000.0)
        else:
            local_state = self._get_local_state(topic)
            async with local_state["lock"]:
                current = local_state["value"]
                if current is None:
                    raise RuntimeError("Cannot update non-existent state")
                state = current
                updater(state)
                local_state["value"] = state

                for watcher in local_state["watchers"]:
                    watcher._update(state)

                return state
    
    async def close(self):
        """Cleanup subscriptions"""
        for task in self._subscriptions.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._subscriptions.clear()
        
        if self.backend:
            await self.backend.close()
