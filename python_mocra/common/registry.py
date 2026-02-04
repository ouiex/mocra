import asyncio
import time
import socket
import logging
import json
from typing import List
from pydantic import BaseModel
from cacheable.service import CacheService

logger = logging.getLogger(__name__)

class NodeInfo(BaseModel):
    id: str
    ip: str
    hostname: str
    last_heartbeat: int
    version: str

class NodeRegistry:
    def __init__(self, cache: CacheService, node_id: str, ttl: int = 30):
        self._cache = cache
        self._node_id = node_id
        self._prefix = "nodes"
        self._ttl = ttl  # seconds
        self._index_key = "registry:nodes_index"

    def _key(self, node_id: str) -> str:
        return f"{self._prefix}:{node_id}"

    async def heartbeat(self):
        try:
            now = int(time.time())
            # In container/cloud, obtaining external IP is harder. 
            # Using gethostbyname(gethostname()) is a reasonable default.
            try:
                ip = socket.gethostbyname(socket.gethostname())
            except Exception:
                ip = "127.0.0.1"

            info = NodeInfo(
                id=self._node_id,
                ip=ip,
                hostname=socket.gethostname(),
                last_heartbeat=now,
                version="0.1.0"
            )
            # 1) Save info with TTL
            await self._cache.set(self._key(self._node_id), info.model_dump_json().encode("utf-8"), ttl=self._ttl)

            # 2) Update ZSET index
            await self._cache.zadd(self._index_key, float(now), self._node_id.encode("utf-8"))
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")

    async def get_active_nodes(self) -> List[NodeInfo]:
        try:
            now = int(time.time())
            cutoff = now - self._ttl

            # Lazy cleanup
            await self._cache.zremrangebyscore(self._index_key, float("-inf"), float(cutoff))

            active_ids_bytes = await self._cache.zrangebyscore(self._index_key, float(cutoff), float("inf"))
            if not active_ids_bytes:
                return []

            keys = []
            for b in active_ids_bytes:
                try:
                    node_id = b.decode("utf-8")
                except Exception:
                    node_id = ""
                if node_id:
                    keys.append(self._key(node_id))

            if not keys:
                return []

            values = await self._cache.mget(keys)
            nodes: List[NodeInfo] = []
            for val in values:
                if not val:
                    continue
                try:
                    node = NodeInfo.model_validate_json(val)
                    nodes.append(node)
                except Exception:
                    pass
            return nodes

        except Exception as e:
            logger.error(f"Failed to get active nodes: {e}")
            return []

    async def start_loop(self):
        logger.info(f"Starting heartbeat loop for node {self._node_id}")
        while True:
            await self.heartbeat()
            await asyncio.sleep(10) # Send every 10s, TTL is 30s

    async def deregister(self):
        logger.info(f"Deregistering node {self._node_id}")
        try:
            await self._cache.delete(self._key(self._node_id))
        except Exception as e:
            logger.error(f"Deregister failed: {e}")
