import asyncio
import logging
from typing import Optional, Type, TypeVar, Any, Dict
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pydantic import BaseModel
from .compression import compress_payload, decompress_payload

from .interface import MqBackend, QueuedItem
from common.models.message import TaskModel, Request, Response, ParserTaskModel

T = TypeVar("T", bound=BaseModel)
logger = logging.getLogger(__name__)

class KafkaBackend(MqBackend):
    def __init__(self, bootstrap_servers: str, compression_threshold: int = 1024):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[AIOKafkaProducer] = None
        # Key: f"{group_id}:{topic}"
        self._consumers: Dict[str, AIOKafkaConsumer] = {} 
        self._compression_threshold = compression_threshold
        
    async def start(self):
        if not self._producer:
            self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
            await self._producer.start()

    async def close(self):
        if self._producer:
            await self._producer.stop()
        for consumer in self._consumers.values():
            await consumer.stop()

    def _topic_name(self, topic_type: str) -> str:
        return f"mocra.{topic_type}"

    async def _publish(self, topic: str, model: BaseModel):
        if not self._producer:
            await self.start()
        # Key by uuid/id if available to ensure ordering for same entity?
        # For now random.
        try:
            payload = model.model_dump_json().encode("utf-8")
            payload = compress_payload(payload, self._compression_threshold)
            await self._producer.send_and_wait(topic, payload)
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")
            raise

    async def _consume(self, topic: str, group: str, consumer_id: str, model_cls: Type[T]) -> Optional[QueuedItem[T]]:
        consumer_key = f"{group}:{topic}"
        
        if consumer_key not in self._consumers:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group,
                client_id=consumer_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False 
            )
            await consumer.start()
            self._consumers[consumer_key] = consumer
            
        consumer = self._consumers[consumer_key]
        
        try:
            # Poll for messages
            # getone() returns one message, waiting indefinitely unless timeout
            msg = await asyncio.wait_for(consumer.getone(), timeout=2.0)
            
            try:
                payload = decompress_payload(msg.value)
                item = model_cls.model_validate_json(payload.decode("utf-8", errors="ignore"))
            except Exception as e:
                logger.error(f"Poison message in {topic} offset {msg.offset}: {e}")
                # Commit to skip
                tp = msg.topic_partition
                await consumer.commit({tp: msg.offset + 1})
                return None
            
            async def ack_func():
                tp = msg.topic_partition
                await consumer.commit({tp: msg.offset + 1})

            async def nack_func(reason: str):
                logger.warning(f"NACK for {topic} msg {msg.offset}: {reason}")
                # For Kafka, we usually just commit (lossy) or produce to DLQ.
                # Emulating Redis behavior (re-delivery) is hard without just not committing, 
                # but that blocks the partition.
                # Here we will just ack to move on, assuming NACK means "failed processing, log it".
                await ack_func()

            return QueuedItem(item, ack_func, nack_func)
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Error consuming from {topic}: {e}")
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
        return await self._consume(self._topic_name("task"), group, consumer, TaskModel)

    async def consume_request(self, group: str, consumer: str) -> Optional[QueuedItem[Request]]:
        return await self._consume(self._topic_name("request"), group, consumer, Request)

    async def consume_response(self, group: str, consumer: str) -> Optional[QueuedItem[Response]]:
        return await self._consume(self._topic_name("response"), group, consumer, Response)

    async def consume_parser_task(self, group: str, consumer: str) -> Optional[QueuedItem[ParserTaskModel]]:
        return await self._consume(self._topic_name("parser_task"), group, consumer, ParserTaskModel)
