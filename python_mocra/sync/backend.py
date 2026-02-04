"""
Coordination Backend - Abstract interface for distributed coordination
"""
from abc import ABC, abstractmethod
from typing import Optional, AsyncIterator

class CoordinationBackend(ABC):
    """
    Abstract backend for distributed coordination operations.
    Provides key-value storage, pub/sub, and distributed locking.
    """
    
    @abstractmethod
    async def get(self, key: str) -> Optional[bytes]:
        """Get value for key"""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: bytes, ttl_ms: Optional[int] = None) -> bool:
        """Set key-value pair with optional TTL"""
        pass
    
    @abstractmethod
    async def publish(self, topic: str, message: bytes) -> bool:
        """Publish message to topic"""
        pass
    
    @abstractmethod
    async def subscribe(self, topic: str) -> AsyncIterator[bytes]:
        """Subscribe to topic and yield messages"""
        pass
    
    @abstractmethod
    async def acquire_lock(self, key: str, value: bytes, ttl_ms: int) -> bool:
        """Try to acquire distributed lock"""
        pass
    
    @abstractmethod
    async def renew_lock(self, key: str, value: bytes, ttl_ms: int) -> bool:
        """Renew lock TTL if we own it"""
        pass
    
    @abstractmethod
    async def cas(self, key: str, old_value: Optional[bytes], new_value: bytes) -> bool:
        """Compare-and-swap operation"""
        pass
