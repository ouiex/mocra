from abc import ABC, abstractmethod
from typing import AsyncGenerator, Dict, Any, Optional, TypeVar, Generic, Callable, Awaitable
from common.models.message import TaskModel, Request, Response, ParserTaskModel

T = TypeVar("T")

class QueuedItem(Generic[T]):
    """
    Wrapper around a message from the queue, allowing Ack/Nack actions.
    """
    def __init__(self, item: T, ack_fn: Callable[[], Awaitable[None]], nack_fn: Callable[[str], Awaitable[None]]):
        self.item = item
        self._ack_fn = ack_fn
        self._nack_fn = nack_fn
        self._action_taken = False

    async def ack(self):
        if self._action_taken:
            return
        await self._ack_fn()
        self._action_taken = True

    async def nack(self, reason: str = "unknown"):
        if self._action_taken:
            return
        await self._nack_fn(reason)
        self._action_taken = True

class MqBackend(ABC):
    """
    Abstract Interface for Message Queue Backends (Redis/Kafka)
    """
    
    @abstractmethod
    async def publish_task(self, task: TaskModel): ...
    
    @abstractmethod
    async def publish_request(self, request: Request): ...
    
    @abstractmethod
    async def publish_response(self, response: Response): ...

    @abstractmethod
    async def publish_parser_task(self, task: ParserTaskModel): ...
        
    @abstractmethod
    async def consume_task(self, group: str, consumer: str) -> Optional[QueuedItem[TaskModel]]: ...
    
    @abstractmethod
    async def consume_request(self, group: str, consumer: str) -> Optional[QueuedItem[Request]]: ...
    
    @abstractmethod
    async def consume_response(self, group: str, consumer: str) -> Optional[QueuedItem[Response]]: ...

    @abstractmethod
    async def consume_parser_task(self, group: str, consumer: str) -> Optional[QueuedItem[ParserTaskModel]]: ...
