import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from fakeredis.aioredis import FakeRedis

from mq.redis_backend import RedisQueueBackend
from common.models.message import TaskModel


@pytest.mark.asyncio
async def test_redis_queue_nack_to_dlq():
    backend = RedisQueueBackend("redis://localhost:6379/0")
    backend._redis = FakeRedis(decode_responses=False)
    backend._nack_script = backend._redis.register_script(
        """
        redis.call('XADD', KEYS[3], '*', 'payload', ARGV[1], 'reason', ARGV[2], 'original_id', ARGV[3])
        return redis.call('XACK', KEYS[1], KEYS[2], ARGV[3])
        """
    )

    task = TaskModel(account="acc", platform="plat")
    await backend.publish_task(task)

    queued = await backend.consume_task("group1", "consumer1")
    assert queued is not None

    await queued.nack("test_reason")

    dlq_key = "queue:task:dlq"
    entries = await backend._redis.xrange(dlq_key, min="-", max="+")

    assert len(entries) == 1
    _, fields = entries[0]
    reason_val = fields.get("reason")
    payload_val = fields.get("payload")
    if reason_val is None:
        reason_val = fields.get(b"reason")
    if payload_val is None:
        payload_val = fields.get(b"payload")

    if isinstance(reason_val, bytes):
        reason_val = reason_val.decode("utf-8")
    if isinstance(payload_val, bytes):
        payload_val = payload_val.decode("utf-8")

    assert reason_val == "test_reason"
    assert payload_val == task.model_dump_json()
