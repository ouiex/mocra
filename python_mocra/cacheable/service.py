"""
Cache Service - Provides caching with Redis or local fallback
"""
import gzip
import logging
import json
import time
from abc import ABC, abstractmethod
from typing import Optional, Type, TypeVar, Any, Dict, List
from datetime import datetime, timedelta
from redis.asyncio import Redis
from pydantic import BaseModel
from common.models.headers import Headers
from common.models.cookies import Cookies
from common.models.response import Response

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class CacheBackend(ABC):
    """Abstract cache backend interface"""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[bytes]:
        """Get value for key"""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: bytes, ttl: Optional[int] = None) -> None:
        """Set key-value with optional TTL in seconds"""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete key"""
        pass
    
    @abstractmethod
    async def keys(self, pattern: str) -> List[str]:
        """Get keys matching pattern"""
        pass

    @abstractmethod
    async def mget(self, keys: List[str]) -> List[Optional[bytes]]:
        """Get multiple keys"""
        pass
    
    @abstractmethod
    async def set_nx(self, key: str, value: bytes, ttl: Optional[int] = None) -> bool:
        """Set if not exists"""
        pass

    @abstractmethod
    async def set_nx_batch(self, keys: List[str], value: bytes, ttl: Optional[int] = None) -> List[bool]:
        """Set multiple keys if not exists"""
        pass
    
    @abstractmethod
    async def incr(self, key: str, delta: int = 1) -> int:
        """Increment counter"""
        pass
    
    @abstractmethod
    async def zadd(self, key: str, score: float, member: bytes) -> int:
        """Add member to sorted set"""
        pass
    
    @abstractmethod
    async def zrangebyscore(self, key: str, min_score: float, max_score: float) -> List[bytes]:
        """Get sorted set members by score range"""
        pass
    
    @abstractmethod
    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        """Remove sorted set members by score range"""
        pass


class RedisBackend(CacheBackend):
    """Redis-based cache backend"""
    
    def __init__(self, redis: Redis):
        self.redis = redis
    
    async def get(self, key: str) -> Optional[bytes]:
        value = await self.redis.get(key)
        return value.encode('utf-8') if isinstance(value, str) else value
    
    async def set(self, key: str, value: bytes, ttl: Optional[int] = None) -> None:
        if ttl:
            await self.redis.setex(key, ttl, value)
        else:
            await self.redis.set(key, value)
    
    async def delete(self, key: str) -> None:
        await self.redis.delete(key)
    
    async def keys(self, pattern: str) -> List[str]:
        return await self.redis.keys(pattern)

    async def mget(self, keys: List[str]) -> List[Optional[bytes]]:
        if not keys:
            return []
        values = await self.redis.mget(keys)
        # Normalize to bytes
        result: List[Optional[bytes]] = []
        for val in values:
            if val is None:
                result.append(None)
            elif isinstance(val, str):
                result.append(val.encode("utf-8"))
            else:
                result.append(val)
        return result
    
    async def set_nx(self, key: str, value: bytes, ttl: Optional[int] = None) -> bool:
        if ttl:
            return await self.redis.set(key, value, nx=True, ex=ttl)
        return await self.redis.setnx(key, value)

    async def set_nx_batch(self, keys: List[str], value: bytes, ttl: Optional[int] = None) -> List[bool]:
        if not keys:
            return []
        pipe = self.redis.pipeline()
        for key in keys:
            if ttl:
                pipe.set(key, value, nx=True, ex=ttl)
            else:
                pipe.set(key, value, nx=True)
        results = await pipe.execute()
        return [bool(r) for r in results]
    
    async def incr(self, key: str, delta: int = 1) -> int:
        return await self.redis.incrby(key, delta)
    
    async def zadd(self, key: str, score: float, member: bytes) -> int:
        return await self.redis.zadd(key, {member: score})
    
    async def zrangebyscore(self, key: str, min_score: float, max_score: float) -> List[bytes]:
        results = await self.redis.zrangebyscore(key, min_score, max_score)
        return [r.encode('utf-8') if isinstance(r, str) else r for r in results]
    
    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        return await self.redis.zremrangebyscore(key, min_score, max_score)


class LocalBackend(CacheBackend):
    """In-memory cache backend for local/standalone mode"""
    
    def __init__(self):
        # store: {key: (value, expiry_timestamp)}
        self.store: Dict[str, tuple[bytes, Optional[float]]] = {}
        # zstore: {key: [(score, member), ...]}
        self.zstore: Dict[str, List[tuple[float, bytes]]] = {}
    
    def _is_expired(self, expiry: Optional[float]) -> bool:
        if expiry is None:
            return False
        return time.time() > expiry
    
    def _cleanup_expired(self, key: str) -> bool:
        """Remove expired entry, return True if was expired"""
        if key in self.store:
            _, expiry = self.store[key]
            if self._is_expired(expiry):
                del self.store[key]
                return True
        return False
    
    async def get(self, key: str) -> Optional[bytes]:
        if self._cleanup_expired(key):
            return None
        if key in self.store:
            value, _ = self.store[key]
            return value
        return None
    
    async def set(self, key: str, value: bytes, ttl: Optional[int] = None) -> None:
        expiry = time.time() + ttl if ttl else None
        self.store[key] = (value, expiry)
    
    async def delete(self, key: str) -> None:
        self.store.pop(key, None)
    
    async def keys(self, pattern: str) -> List[str]:
        # Simple prefix matching
        prefix = pattern.rstrip('*')
        result = []
        for k in list(self.store.keys()):
            if self._cleanup_expired(k):
                continue
            if k.startswith(prefix):
                result.append(k)
        return result

    async def mget(self, keys: List[str]) -> List[Optional[bytes]]:
        results: List[Optional[bytes]] = []
        for key in keys:
            if self._cleanup_expired(key):
                results.append(None)
                continue
            if key in self.store:
                value, _ = self.store[key]
                results.append(value)
            else:
                results.append(None)
        return results
    
    async def set_nx(self, key: str, value: bytes, ttl: Optional[int] = None) -> bool:
        if self._cleanup_expired(key):
            # Was expired, can set
            await self.set(key, value, ttl)
            return True
        if key not in self.store:
            await self.set(key, value, ttl)
            return True
        return False

    async def set_nx_batch(self, keys: List[str], value: bytes, ttl: Optional[int] = None) -> List[bool]:
        results: List[bool] = []
        for key in keys:
            result = await self.set_nx(key, value, ttl)
            results.append(result)
        return results
    
    async def incr(self, key: str, delta: int = 1) -> int:
        current = 0
        if not self._cleanup_expired(key) and key in self.store:
            value, _ = self.store[key]
            try:
                current = int(value.decode('utf-8'))
            except:
                current = 0
        new_val = current + delta
        await self.set(key, str(new_val).encode('utf-8'))
        return new_val
    
    async def zadd(self, key: str, score: float, member: bytes) -> int:
        if key not in self.zstore:
            self.zstore[key] = []
        # Remove if already exists
        self.zstore[key] = [(s, m) for s, m in self.zstore[key] if m != member]
        self.zstore[key].append((score, member))
        self.zstore[key].sort(key=lambda x: x[0])
        return 1
    
    async def zrangebyscore(self, key: str, min_score: float, max_score: float) -> List[bytes]:
        if key not in self.zstore:
            return []
        return [m for s, m in self.zstore[key] if min_score <= s <= max_score]
    
    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        if key not in self.zstore:
            return 0
        original_len = len(self.zstore[key])
        self.zstore[key] = [(s, m) for s, m in self.zstore[key] if not (min_score <= s <= max_score)]
        return original_len - len(self.zstore[key])


class CacheService:
    """
    High-level cache service with compression and serialization.
    Supports both Redis and local backends.
    """
    
    def __init__(
        self,
        backend: CacheBackend,
        default_ttl: int = 3600,
        namespace: str = "cache"
    ):
        self.backend = backend
        self.default_ttl = default_ttl
        self.namespace = namespace

    def _make_key(self, key: str) -> str:
        """Add namespace prefix"""
        if self.namespace:
            return f"{self.namespace}:{key}"
        return key

    async def get(self, key: str) -> Optional[bytes]:
        """Get raw bytes"""
        full_key = self._make_key(key)
        return await self.backend.get(full_key)

    async def set(self, key: str, value: bytes, ttl: Optional[int] = None) -> None:
        """Set raw bytes"""
        full_key = self._make_key(key)
        ttl = ttl if ttl is not None else self.default_ttl
        await self.backend.set(full_key, value, ttl)

    async def delete(self, key: str) -> None:
        """Delete key"""
        full_key = self._make_key(key)
        await self.backend.delete(full_key)

    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern (without namespace prefix in results)"""
        full_pattern = self._make_key(pattern)
        keys = await self.backend.keys(full_pattern)
        # Strip namespace prefix from results
        if self.namespace:
            prefix = f"{self.namespace}:"
            return [k[len(prefix):] if k.startswith(prefix) else k for k in keys]
        return keys

    async def mget(self, keys: List[str]) -> List[Optional[bytes]]:
        """Get multiple keys (without namespace prefix in inputs)"""
        full_keys = [self._make_key(key) for key in keys]
        return await self.backend.mget(full_keys)

    async def set_nx(self, key: str, value: bytes, ttl: Optional[int] = None) -> bool:
        """Set if not exists"""
        full_key = self._make_key(key)
        ttl = ttl if ttl is not None else self.default_ttl
        return await self.backend.set_nx(full_key, value, ttl)

    async def set_nx_batch(self, keys: List[str], value: bytes, ttl: Optional[int] = None) -> List[bool]:
        """Set multiple keys if not exists"""
        ttl = ttl if ttl is not None else self.default_ttl
        full_keys = [self._make_key(key) for key in keys]
        return await self.backend.set_nx_batch(full_keys, value, ttl)

    async def get_json(self, key: str, model: Type[T]) -> Optional[T]:
        """Get and deserialize JSON object"""
        try:
            data = await self.get(key)
            if not data:
                return None
            
            # Try decompression
            try:
                decompressed = gzip.decompress(data)
                json_str = decompressed.decode('utf-8')
            except:
                json_str = data.decode('utf-8')
            
            return model.model_validate_json(json_str)
        except Exception as e:
            logger.warning(f"Cache get_json failed for {key}: {e}")
            return None

    async def set_json(self, key: str, value: BaseModel, ttl: Optional[int] = None, compress: bool = True):
        """Serialize and cache JSON object"""
        try:
            json_str = value.model_dump_json()
            if compress:
                data = gzip.compress(json_str.encode('utf-8'))
            else:
                data = json_str.encode('utf-8')
            
            await self.set(key, data, ttl)
        except Exception as e:
            logger.warning(f"Cache set_json failed for {key}: {e}")

    async def save_headers(self, module_id: str, headers: Headers, ttl: Optional[int] = None) -> None:
        await self.set_json(f"headers:{module_id}", headers, ttl=ttl, compress=True)

    async def get_headers(self, module_id: str) -> Optional[Headers]:
        return await self.get_json(f"headers:{module_id}", Headers)

    async def save_cookies(self, module_id: str, cookies: Cookies, ttl: Optional[int] = None) -> None:
        await self.set_json(f"cookies:{module_id}", cookies, ttl=ttl, compress=True)

    async def get_cookies(self, module_id: str) -> Optional[Cookies]:
        return await self.get_json(f"cookies:{module_id}", Cookies)

    async def save_response(self, request_hash: str, response: Response, ttl: Optional[int] = None) -> None:
        await self.set_json(f"response:{request_hash}", response, ttl=ttl, compress=True)

    async def get_response(self, request_hash: str) -> Optional[Response]:
        return await self.get_json(f"response:{request_hash}", Response)

    async def zadd(self, key: str, score: float, member: bytes) -> int:
        full_key = self._make_key(key)
        return await self.backend.zadd(full_key, score, member)

    async def zrangebyscore(self, key: str, min_score: float, max_score: float) -> List[bytes]:
        full_key = self._make_key(key)
        return await self.backend.zrangebyscore(full_key, min_score, max_score)

    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        full_key = self._make_key(key)
        return await self.backend.zremrangebyscore(full_key, min_score, max_score)

