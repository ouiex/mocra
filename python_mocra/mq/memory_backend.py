import asyncio
from typing import Optional, Dict, List, Type, TypeVar
from collections import defaultdict
from pydantic import BaseModel
from .interface import MqBackend, QueuedItem
from common.models.message import TaskModel, Request, Response, ParserTaskModel

T = TypeVar("T", bound=BaseModel)

class MemoryQueueBackend(MqBackend):
    """
    In-memory implementation of MqBackend using asyncio.Queue.
    Useful for local development and testing without Redis.
    """
    def __init__(self):
        self._queues: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._dlq: List[BaseModel] = []
        
    def _topic_name(self, topic_type: str) -> str:
        return f"topic:{topic_type}"

    async def _publish(self, topic: str, model: BaseModel):
        await self._queues[topic].put(model)

    async def _consume(self, topic: str, consumer: str, model_cls: Type[T]) -> Optional[QueuedItem[T]]:
        queue = self._queues[topic]
        if queue.empty():
            return None
        
        try:
            item = queue.get_nowait()
            
            async def ack_func():
                # In memory, popping it off the queue is effectively acking it.
                pass
                
            async def nack_func(reason: str):
                # For memory queue, nack pushes it to DLQ or back to queue?
                # Let's push to DLQ for simplicity in this mock
                # Ideally we wrap it with reason
                self._dlq.append(item)

            return QueuedItem(item, ack_func, nack_func)
            
        except asyncio.QueueEmpty:
            return None

    async def publish_task(self, task: TaskModel):
        await self._publish(self._topic_name("task"), task)

    async def publish_request(self, request: Request):
        await self._publish(self._topic_name("request"), request)

    async def publish_response(self, response: Response):
        await self._publish(self._topic_name("response"), response)

    async def publish_parser_task(self, task: ParserTaskModel):
        await self._publish(self._topic_name("parser_task"), task)

    async def consume_task(self, group: str, consumer: str) -> Optional[QueuedItem[TaskModel]]:
        return await self._consume(self._topic_name("task"), consumer, TaskModel)

    async def consume_request(self, group: str, consumer: str) -> Optional[QueuedItem[Request]]:
        return await self._consume(self._topic_name("request"), consumer, Request)

    async def consume_response(self, group: str, consumer: str) -> Optional[QueuedItem[Response]]:
        return await self._consume(self._topic_name("response"), consumer, Response)

    async def consume_parser_task(self, group: str, consumer: str) -> Optional[QueuedItem[ParserTaskModel]]:
        return await self._consume(self._topic_name("parser_task"), consumer, ParserTaskModel)
