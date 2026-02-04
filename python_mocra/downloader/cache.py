import gzip
import json
import logging
from typing import Optional
from redis.asyncio import Redis
from common.models.message import Response

logger = logging.getLogger(__name__)

class ResponseCache:
    def __init__(self, redis: Redis, default_ttl: int = 3600):
        self._redis = redis
        self._default_ttl = default_ttl
        self._prefix = "cache:response"

    def _key(self, request_hash: str) -> str:
        return f"{self._prefix}:{request_hash}"

    async def get_response(self, request_hash: str) -> Optional[Response]:
        try:
            data = await self._redis.get(self._key(request_hash))
            if not data:
                return None
            
            # Decompress
            decompressed = gzip.decompress(data)
            # Parse JSON
            # We assume stored data is serialized Response
            return Response.model_validate_json(decompressed.decode('utf-8'))
        except Exception as e:
            logger.warning(f"Cache get failed for {request_hash}: {e}")
            return None

    async def save_response(self, request_hash: str, response: Response, ttl: Optional[int] = None):
        try:
            # Serialize
            json_str = response.model_dump_json()
            # Compress
            compressed = gzip.compress(json_str.encode('utf-8'))
            
            expiration = ttl if ttl is not None else self._default_ttl
            
            await self._redis.set(
                self._key(request_hash),
                compressed,
                ex=expiration
            )
        except Exception as e:
            logger.warning(f"Cache save failed for {request_hash}: {e}")
