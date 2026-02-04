import os
import sys
import pytest
import asyncio

# ensure project root on path for imports
sys.path.append(os.getcwd())

from cacheable.service import CacheService, LocalBackend
from common.registry import NodeRegistry


@pytest.mark.asyncio
async def test_node_registry_heartbeat_and_active_nodes():
    cache = CacheService(LocalBackend(), namespace="test_ns")
    registry = NodeRegistry(cache, node_id="node-1", ttl=2)

    await registry.heartbeat()

    nodes = await registry.get_active_nodes()
    assert len(nodes) == 1
    assert nodes[0].id == "node-1"

    # wait beyond TTL, node should expire from index
    await asyncio.sleep(3)
    nodes_after = await registry.get_active_nodes()
    assert nodes_after == []
