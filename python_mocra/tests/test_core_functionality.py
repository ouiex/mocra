"""
Test script to verify core functionality of Python mocra
"""
import asyncio
import logging
from uuid6 import uuid7

from common.state import get_state
from common.models.message import TaskModel
from cacheable.service import LocalBackend, CacheService
from sync.distributed import SyncService, SyncAble
from sync.leader import LeaderElector
from mq.memory_backend import MemoryQueueBackend

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Test SyncAble implementation
class TestConfig(SyncAble):
    def __init__(self, rate: int):
        self.rate = rate
    
    @classmethod
    def topic(cls) -> str:
        return "test_config"


async def test_cache_service():
    """Test CacheService with local backend"""
    logger.info("=== Testing CacheService ===")
    
    backend = LocalBackend()
    cache = CacheService(backend, namespace="test")
    
    # Test basic set/get
    await cache.set("key1", b"value1", ttl=60)
    result = await cache.get("key1")
    assert result == b"value1", f"Expected b'value1', got {result}"
    logger.info("✓ Basic set/get works")
    
    # Test set_nx
    was_set = await cache.set_nx("key2", b"value2", ttl=60)
    assert was_set is True
    was_set_again = await cache.set_nx("key2", b"value3", ttl=60)
    assert was_set_again is False
    logger.info("✓ set_nx works")
    
    # Test keys
    await cache.set("test:1", b"v1")
    await cache.set("test:2", b"v2")
    keys = await cache.keys("test:*")
    assert len(keys) >= 2
    logger.info(f"✓ keys() works: {keys}")
    
    logger.info("CacheService tests passed!\n")


async def test_sync_service():
    """Test SyncService in local mode"""
    logger.info("=== Testing SyncService (Local Mode) ===")
    
    sync_service = SyncService(backend=None, namespace="test")
    
    # Create sync object
    sync_handle = await sync_service.sync(TestConfig)
    
    # Initially should be None
    value = sync_handle.get()
    assert value is None
    logger.info("✓ Initial value is None")
    
    # Publish a value
    config = TestConfig(rate=100)
    await sync_service.publish(config)
    
    # Give a moment for local propagation
    await asyncio.sleep(0.1)
    
    # Should now have the value
    value = sync_handle.get()
    assert value is not None
    assert value.rate == 100
    logger.info(f"✓ Published and received: rate={value.rate}")
    
    await sync_service.close()
    logger.info("SyncService tests passed!\n")


async def test_leader_elector():
    """Test LeaderElector in local mode"""
    logger.info("=== Testing LeaderElector (Local Mode) ===")
    
    # Local mode (no redis)
    elector = LeaderElector(None, "test_leader", ttl_ms=5000)
    await elector.start()
    
    # In local mode, should always be leader
    assert elector.is_leader is True
    logger.info("✓ Local mode: Always leader")
    
    await elector.stop()
    logger.info("LeaderElector tests passed!\n")


async def test_mq_backend():
    """Test Memory Queue Backend"""
    logger.info("=== Testing MemoryQueueBackend ===")
    
    mq = MemoryQueueBackend()
    
    # Publish a task
    task = TaskModel(
        account="test_account",
        platform="test_platform",
        module=["test_module"],
        run_id=uuid7()
    )
    
    await mq.publish_task(task)
    logger.info(f"✓ Published task: {task.task_id}")
    
    # Consume the task
    queued_item = await mq.consume_task("default", "worker1")
    assert queued_item is not None
    assert queued_item.item.task_id == task.task_id
    logger.info(f"✓ Consumed task: {queued_item.item.task_id}")
    
    # Ack the task
    await queued_item.ack()
    logger.info("✓ ACKed task")
    
    # Queue should be empty now
    queued_item2 = await mq.consume_task("default", "worker1")
    assert queued_item2 is None
    logger.info("✓ Queue is empty after consumption")
    
    logger.info("MemoryQueueBackend tests passed!\n")


async def main():
    """Run all tests"""
    logger.info("Starting Python mocra core functionality tests...\n")
    
    try:
        await test_cache_service()
        await test_sync_service()
        await test_leader_elector()
        await test_mq_backend()
        
        logger.info("=" * 50)
        logger.info("✅ All tests passed!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
