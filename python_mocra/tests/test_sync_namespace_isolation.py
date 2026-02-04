import os
import sys
import pytest
from dataclasses import dataclass

# ensure project root on path for imports
sys.path.append(os.getcwd())

from fakeredis.aioredis import FakeRedis

from sync.distributed import SyncService, SyncAble
from sync.redis_backend import RedisCoordinationBackend


@dataclass
class TestState(SyncAble):
    __test__ = False
    val: str

    @classmethod
    def topic(cls) -> str:
        return "isolation_test"


@pytest.mark.asyncio
async def test_local_namespace_isolation():
    # reset local store to keep tests isolated
    SyncService._local_store = {}

    service1 = SyncService(None, namespace="ns1")
    service2 = SyncService(None, namespace="ns2")

    state1 = TestState(val="data_for_ns1")
    state2 = TestState(val="data_for_ns2")

    await service1.publish(state1)
    await service2.publish(state2)

    fetched1 = await service1.fetch_latest(TestState)
    fetched2 = await service2.fetch_latest(TestState)

    assert fetched1 == state1
    assert fetched2 == state2
    assert fetched1 != fetched2


@pytest.mark.asyncio
async def test_distributed_namespace_isolation():
    backend = RedisCoordinationBackend("redis://localhost:6379/0")
    backend._redis = FakeRedis(decode_responses=False)

    ns1 = "dist_ns1"
    ns2 = "dist_ns2"
    service1 = SyncService(backend, namespace=ns1)
    service2 = SyncService(backend, namespace=ns2)

    state1 = TestState(val="dist_data_1")
    state2 = TestState(val="dist_data_2")

    await service1.publish(state1)
    await service2.publish(state2)

    fetched1 = await service1.fetch_latest(TestState)
    fetched2 = await service2.fetch_latest(TestState)

    assert fetched1 == state1
    assert fetched2 == state2

    # update only service1's namespace
    await service1.optimistic_update(TestState, lambda s: setattr(s, "val", "dist_updated_1"))

    fetched1_after = await service1.fetch_latest(TestState)
    fetched2_after = await service2.fetch_latest(TestState)

    assert fetched1_after.val == "dist_updated_1"
    assert fetched2_after == state2
