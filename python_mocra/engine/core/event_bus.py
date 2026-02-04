import asyncio
import logging
from typing import Callable, Dict, List, Awaitable, Optional
from .events import BaseEvent

logger = logging.getLogger(__name__)

EventHandler = Callable[[BaseEvent], Awaitable[None]]

class EventBus:
    def __init__(self):
        self._handlers: Dict[str, List[EventHandler]] = {}
        self._queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None

    def subscribe(self, event_type: str, handler: EventHandler):
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def publish(self, event: BaseEvent):
        # Fire and forget mechanism via queue
        self._queue.put_nowait(event)

    async def start(self):
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._process_queue())
        logger.info("EventBus started")

    async def stop(self):
        self._running = False
        if self._worker_task:
            await self._queue.join()
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("EventBus stopped")

    async def _process_queue(self):
        while self._running:
            try:
                event: BaseEvent = await self._queue.get()
                
                # Dynamic dispatch based on event_type field
                event_type = getattr(event, "event_type", None)
                if not event_type:
                    # Fallback to class name if not set? or ignore
                    logger.warning(f"Event {event} missing event_type")
                    self._queue.task_done()
                    continue

                handlers = self._handlers.get(event_type, [])
                # Also support wildcard handlers?
                handlers += self._handlers.get("*", [])

                if handlers:
                    # Run handlers concurrently
                    await asyncio.gather(*[h(event) for h in handlers], return_exceptions=True)
                
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in EventBus processing: {e}")

# Global EventBus instance could be useful, or attached to State
from common.state import get_state

def get_event_bus() -> 'EventBus':
    state = get_state()
    # If we attach it to state. But state is dynamic.
    # We will initialize it in main and attach to state.
    if hasattr(state, 'event_bus') and state.event_bus:
        return state.event_bus
    raise RuntimeError("EventBus not initialized")
