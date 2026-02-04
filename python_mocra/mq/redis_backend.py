import asyncio
import logging
import random
import zlib
from enum import Enum
from typing import Optional, Type, TypeVar, Any, Set, Dict, List
from uuid import UUID
from redis.asyncio import Redis, ConnectionPool
from pydantic import BaseModel
from .compression import compress_payload, decompress_payload

from .interface import MqBackend, QueuedItem
from common.models.message import TaskModel, Request, Response, ParserTaskModel, ErrorTaskModel
from common.models.priority import Priority

try:
    import msgpack
except Exception:  # pragma: no cover
    msgpack = None

T = TypeVar("T", bound=BaseModel)
logger = logging.getLogger(__name__)

class RedisQueueBackend(MqBackend):
    def __init__(
        self,
        redis_url: str,
        namespace: str,
        shards: int = 1,
        group_id: Optional[str] = None,
        compression_threshold: int = 1024,
        codec: Optional[str] = None,
    ):
        self._pool = ConnectionPool.from_url(redis_url, decode_responses=False)
        self._redis = Redis(connection_pool=self._pool)
        self._initialized_groups: Set[str] = set()
        self._compression_threshold = compression_threshold
        self._namespace = namespace
        self._shards = max(shards, 1)
        self._group_id = group_id or f"{namespace}:crawler_group"
        self._codec = (codec or "json").lower()
        if self._codec in {"msgpack", "rmp"} and msgpack is None:
            raise RuntimeError("msgpack is required for binary queue codec")
        
        # Load Lua scripts
        # [stream_key, group_id, dlq_key], [payload, reason, id]
        self._nack_script = self._redis.register_script("""
            redis.call('XADD', KEYS[3], '*', 'payload', ARGV[1], 'reason', ARGV[2], 'original_id', ARGV[3])
            return redis.call('XACK', KEYS[1], KEYS[2], ARGV[3])
        """)

    async def close(self):
        await self._redis.close()
        await self._pool.disconnect()

    @classmethod
    def from_config(cls, config) -> "RedisQueueBackend":
        channel = getattr(config, "channel_config", None)
        redis_cfg = channel.redis if channel and channel.redis else None

        if redis_cfg:
            redis_url = redis_cfg.url
            namespace = getattr(config, "name", "mocra")
            shards = redis_cfg.shards or 1
            compression = channel.compression_threshold or 1024
            codec = channel.queue_codec if channel else None
            return cls(redis_url, namespace=namespace, shards=shards, compression_threshold=compression, codec=codec)

        redis_url = config.redis.url
        namespace = getattr(config, "name", "mocra")
        codec = config.channel_config.queue_codec if config.channel_config else None
        return cls(redis_url, namespace=namespace, codec=codec)

    def _topic_name(self, topic_type: str, priority: Priority) -> str:
        return f"{topic_type}-{priority.suffix}"

    def _get_topic_key(self, topic: str, key: Optional[str]) -> str:
        if self._shards > 1:
            if key:
                shard = zlib.crc32(key.encode("utf-8")) % self._shards
            else:
                shard = random.randint(0, self._shards - 1)
            return f"{{{self._namespace}:{topic}:{shard}}}"
        return f"{{{self._namespace}:{topic}}}"

    def _resolve_group(self, group: str) -> str:
        return self._group_id or group

    def _topic_keys(self, topic: str) -> List[str]:
        if self._shards > 1:
            return [f"{{{self._namespace}:{topic}:{i}}}" for i in range(self._shards)]
        return [f"{{{self._namespace}:{topic}}}"]

    async def _ensure_group(self, stream_keys: List[str], group: str):
        for stream_key in stream_keys:
            cache_key = f"{stream_key}::{group}"
            if cache_key in self._initialized_groups:
                continue

            try:
                await self._redis.xgroup_create(stream_key, group, id="0", mkstream=True)
                self._initialized_groups.add(cache_key)
            except Exception as e:
                if "BUSYGROUP" in str(e):
                    self._initialized_groups.add(cache_key)
                else:
                    logger.error(f"Error creating group {group} for {stream_key}: {e}")
                    raise

    async def _publish(self, topic: str, model: BaseModel, key: Optional[str]):
        payload = self._encode_model(model)
        payload = compress_payload(payload, self._compression_threshold)
        topic_key = self._get_topic_key(topic, key)
        await self._redis.xadd(topic_key, {"payload": payload})

    async def _consume_topic(self, topic: str, group: str, consumer: str, model_cls: Type[T]) -> Optional[QueuedItem[T]]:
        topic_keys = self._topic_keys(topic)
        await self._ensure_group(topic_keys, group)
        
        try:
            # block=2000 (2s) to avoid busy loop in consumers
            streams = {key: ">" for key in topic_keys}
            resp = await self._redis.xreadgroup(group, consumer, streams, count=1, block=2000)
            if not resp:
                return None
            
            # resp is [[topic, [(id, fields), ...]], ...]
            stream_key, messages = resp[0]
            if not messages:
                return None
                
            msg_id, fields = messages[0]
            payload_val = fields.get("payload")
            if payload_val is None:
                payload_val = fields.get(b"payload")
            
            if not payload_val:
                logger.error(f"Missing payload in msg {msg_id} from {topic}")
                await self._redis.xack(topic, group, msg_id)
                return None

            if isinstance(payload_val, str):
                payload_bytes = payload_val.encode("utf-8")
            else:
                payload_bytes = payload_val

            payload_bytes = decompress_payload(payload_bytes)

            try:
                item = self._decode_model(payload_bytes, model_cls)
            except Exception as e:
                logger.error(f"Poison message {msg_id} in {topic}: {e}")
                dlq_key = f"{stream_key}:dlq"
                # Auto-nack poison message with reason
                await self._nack_script(keys=[stream_key, group, dlq_key], args=[payload_bytes, f"parse_error:{str(e)}", msg_id])
                return None

            async def ack_func():
                await self._redis.xack(stream_key, group, msg_id)

            async def nack_func(reason: str):
                try:
                    dlq_key = f"{stream_key}:dlq"
                    await self._nack_script(keys=[stream_key, group, dlq_key], args=[payload_bytes, reason, msg_id])
                except Exception as e:
                    logger.error(f"Failed to Nack {msg_id}: {e}")

            return QueuedItem(item, ack_func, nack_func)

        except Exception as e:
            logger.error(f"Error consuming from {topic}: {e}")
            return None

    def _get_key(self, model: BaseModel) -> Optional[str]:
        if hasattr(model, "id"):
            return str(getattr(model, "id"))
        if hasattr(model, "task_id"):
            return str(getattr(model, "task_id"))
        return None

    def _get_priority(self, model: BaseModel) -> Priority:
        if hasattr(model, "get_priority"):
            return model.get_priority()
        if hasattr(model, "priority"):
            return getattr(model, "priority")
        return Priority.NORMAL

    async def publish_task(self, task: TaskModel):
        topic = self._topic_name("task", self._get_priority(task))
        await self._publish(topic, task, self._get_key(task))

    async def publish_request(self, request: Request):
        topic = self._topic_name("request", self._get_priority(request))
        await self._publish(topic, request, self._get_key(request))

    async def publish_response(self, response: Response):
        topic = self._topic_name("response", self._get_priority(response))
        await self._publish(topic, response, self._get_key(response))

    async def publish_parser_task(self, task: ParserTaskModel):
        topic = self._topic_name("parser_task", self._get_priority(task))
        await self._publish(topic, task, self._get_key(task))

    async def _consume(self, topic_base: str, group: str, consumer: str, model_cls: Type[T]) -> Optional[QueuedItem[T]]:
        group_id = self._resolve_group(group)
        for priority in [Priority.HIGH, Priority.NORMAL, Priority.LOW]:
            topic = self._topic_name(topic_base, priority)
            item = await self._consume_topic(topic, group_id, consumer, model_cls)
            if item is not None:
                return item
        return None

    async def consume_task(self, group: str, consumer: str) -> Optional[QueuedItem[TaskModel]]:
        return await self._consume("task", group, consumer, TaskModel)

    async def consume_request(self, group: str, consumer: str) -> Optional[QueuedItem[Request]]:
        return await self._consume("request", group, consumer, Request)

    async def consume_response(self, group: str, consumer: str) -> Optional[QueuedItem[Response]]:
        return await self._consume("response", group, consumer, Response)

    async def consume_parser_task(self, group: str, consumer: str) -> Optional[QueuedItem[ParserTaskModel]]:
        return await self._consume("parser_task", group, consumer, ParserTaskModel)

    def _encode_model(self, model: BaseModel) -> bytes:
        if self._codec in {"msgpack", "rmp"}:
            data = self._to_msgpack(model)
            return msgpack.packb(data, use_bin_type=True)
        return model.model_dump_json().encode("utf-8")

    def _decode_model(self, payload: bytes, model_cls: Type[T]) -> T:
        if self._codec in {"msgpack", "rmp"}:
            raw = msgpack.unpackb(payload, raw=False)
            normalized = self._normalize_msgpack(raw, model_cls)
            return model_cls.model_validate(normalized)
        payload_str = payload.decode("utf-8", errors="ignore")
        return model_cls.model_validate_json(payload_str)

    def _to_msgpack(self, value: Any) -> Any:
        if isinstance(value, BaseModel):
            return self._to_msgpack(value.model_dump(mode="python"))
        if isinstance(value, UUID):
            return value.bytes
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {k: self._to_msgpack(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._to_msgpack(v) for v in value]
        return value

    def _normalize_msgpack(self, value: Any, model_cls: Type[T]) -> Any:
        if not isinstance(value, dict):
            return value

        uuid_fields = {
            TaskModel: {"run_id"},
            Request: {"id", "run_id", "prefix_request"},
            Response: {"id", "run_id", "prefix_request"},
            ParserTaskModel: {"id", "run_id", "prefix_request"},
            ErrorTaskModel: {"id", "run_id", "prefix_request"},
        }.get(model_cls, set())

        def convert(obj: Any) -> Any:
            if isinstance(obj, dict):
                out: Dict[str, Any] = {}
                for k, v in obj.items():
                    if k in uuid_fields and isinstance(v, (bytes, bytearray)) and len(v) == 16:
                        out[k] = UUID(bytes=bytes(v))
                    else:
                        out[k] = convert(v)
                return out
            if isinstance(obj, list):
                return [convert(v) for v in obj]
            return obj

        normalized = convert(value)
        if model_cls in {ParserTaskModel, ErrorTaskModel}:
            if isinstance(normalized, dict) and "account_task" in normalized:
                normalized["account_task"] = self._normalize_msgpack(normalized["account_task"], TaskModel)
        return normalized
