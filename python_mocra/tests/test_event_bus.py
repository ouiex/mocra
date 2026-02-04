import os
import sys
import pytest
import asyncio

# ensure project root on path for imports
sys.path.append(os.getcwd())

from engine.core.event_bus import EventBus
from engine.core.events import EventTaskReceived, TaskEventPayload


@pytest.mark.asyncio
async def test_event_bus_dispatch():
    bus = EventBus()
    events = []

    async def handler(evt):
        events.append(evt)

    bus.subscribe("task.received", handler)
    await bus.start()

    bus.publish(
        EventTaskReceived(
            payload=TaskEventPayload(
                task_id="t1",
                account="a",
                platform="p",
                run_id="r1",
            )
        )
    )

    await asyncio.sleep(0.05)
    await bus.stop()

    assert len(events) == 1
    assert events[0].payload.task_id == "t1"
