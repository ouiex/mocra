import os
import sys
import pytest
from dataclasses import dataclass

# ensure project root on path for imports
sys.path.append(os.getcwd())

from sync.distributed import SyncService, SyncAble


@dataclass
class CounterState(SyncAble):
    __test__ = False
    counter: int

    @classmethod
    def topic(cls) -> str:
        return "counter_state"


@pytest.mark.asyncio
async def test_optimistic_update_local_mode():
    SyncService._local_store = {}
    service = SyncService(None, namespace="local_opt")

    initial = CounterState(counter=0)
    await service.send(initial)

    updated = await service.optimistic_update(CounterState, lambda s: setattr(s, "counter", s.counter + 1))
    assert updated.counter == 1

    latest = await service.fetch_latest(CounterState)
    assert latest.counter == 1
