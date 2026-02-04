import asyncio
import time
import logging
import sys
import os
from uuid6 import uuid7
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

from common.state import get_state
from common.models.message import TaskModel, Request
from common.models.response import Response
from common.models.data import Data, DataType
from common.models.cookies import Cookies
from common.models.entity import AccountModel, PlatformModel
from engine.worker import UnifiedWorker
from mq.redis_backend import RedisQueueBackend
from engine.components.module_manager import ModuleManager
from engine.components.middleware_manager import MiddlewareManager
from common.config import settings
from engine.task.task import Task
from engine.task.module import Module

# Configure Logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# --- Mock Components ---

class LatencyDownloaderManager:
    """
    Simulates a downloader with network latency.
    """
    async def start(self): pass
    async def close(self): pass
    async def download(self, request: Request) -> Response:
        # Simulate network latency (e.g., 50ms)
        await asyncio.sleep(0.05) 
        return Response(
            id=request.id,
            platform=request.platform,
            account=request.account,
            module=request.module,
            status_code=200,
            cookies=Cookies(),
            content=b'{"slide": "slideshow", "title": "Sample Slide Show"}',
            headers=[("Content-Type", "application/json")],
            task_retry_times=request.task_retry_times,
            metadata=request.meta,
            download_middleware=request.download_middleware,
            data_middleware=request.data_middleware,
            task_finished=request.task_finished,
            context=request.context,
            run_id=request.run_id,
            prefix_request=request.id,
            priority=request.priority,
        )

# --- Concurrent Worker ---
class ConcurrentWorker(UnifiedWorker):
    """
    A worker that processes items concurrently to mask I/O latency.
    """
    def __init__(self, concurrency: int = 50, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.concurrency = concurrency

    async def start(self):
        self.running = True
        logger.warning(f"ConcurrentWorker started with {self.concurrency} threads.")
        
        # Spawn N workers
        workers = [asyncio.create_task(self._worker_loop(i)) for i in range(self.concurrency)]
        await asyncio.gather(*workers)

    async def _worker_loop(self, idx: int):
        while self.running:
            did_work = False
            try:
                # 1. Consume Tasks
                if await self._process_task():
                    did_work = True
                
                # 2. Consume Requests (Download)
                if await self._process_request():
                    did_work = True
                    
                # 3. Consume Responses (Parse)
                if await self._process_response():
                    did_work = True
            except Exception as e:
                logger.error(f"Worker {idx} error: {e}")
                await asyncio.sleep(1)

            if not did_work:
                await asyncio.sleep(0.1)

# --- Benchmark Script ---

async def run_benchmark(task_count: int = 500, workers: int = 20):
    print(f"Starting Real-World Simulation Benchmark with {task_count} tasks, {workers} concurrent workers...")
    print(f"Using Redis Backend (ensure Redis is running on localhost:6379)")
    
    state = get_state()
    
    # 1. Initialize Real Redis Backend (with Fallback to FakeRedis)
    try:
        try:
            # Try real connection first
            state.mq = RedisQueueBackend("redis://localhost:6379/0")
            await state.mq._redis.ping()
            print("Connected to Real Redis.")
        except Exception:
            print("Real Redis not available, using FakeRedis (In-Memory Simulation).")
            from fakeredis.aioredis import FakeRedis
            # Monkey patch backend instance
            state.mq = RedisQueueBackend("redis://localhost:6379/0")
            state.mq._redis = FakeRedis(decode_responses=False)
            # Rebind nack script
            state.mq._nack_script = state.mq._redis.register_script("""
                redis.call('XADD', KEYS[3], '*', 'payload', ARGV[1], 'reason', ARGV[2], 'original_id', ARGV[3])
                return redis.call('XACK', KEYS[1], KEYS[2], ARGV[3])
            """)

        state.downloader = LatencyDownloaderManager()
        
        # Clean Redis Keys for fresh start
        await state.mq._redis.delete("queue:task", "queue:request", "queue:response")
        print("Redis queues cleared.")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return

    # Load Modules
    state.module_manager = ModuleManager()
    from modules.demo import DemoSpider
    state.module_manager._modules["demo_spider"] = DemoSpider()

    class BenchmarkTaskManager:
        def __init__(self, state_ref):
            self.state = state_ref

        async def load_with_model(self, task_model: TaskModel) -> Task:
            account = AccountModel(name=task_model.account)
            platform = PlatformModel(name=task_model.platform)

            if task_model.module:
                module_names = task_model.module
            else:
                module_names = self.state.module_manager.list_modules()

            modules = []
            for module_name in module_names:
                base_module = self.state.module_manager.get_module(module_name)
                if not base_module:
                    continue
                modules.append(
                    Module(
                        module_instance=base_module,
                        config={},
                        account=account,
                        platform=platform,
                        run_id=task_model.run_id,
                        prefix_request=uuid7(),
                    )
                )

            return Task(
                account=account,
                platform=platform,
                modules=modules,
                metadata=task_model.model_dump(),
                run_id=task_model.run_id,
                prefix_request=uuid7(),
            )

    state.task_manager = BenchmarkTaskManager(state)
    
    # Setup Middleware to count results
    class CounterMiddleware:
        name = "CounterMiddleware"
        def __init__(self):
            self.count = 0
            self.start_time = 0
            self.end_time = 0
            self.target = task_count
            self.event = asyncio.Event()

        async def handle_data(self, data: Data) -> Data:
            self.count += 1
            if self.count % 100 == 0:
                print(f"Processed: {self.count}/{self.target}")
            if self.count >= self.target:
                self.end_time = time.time()
                self.event.set()
            return data
            
        async def store_data(self, data: Data) -> None:
            pass

    counter = CounterMiddleware()
    state.middleware_manager = MiddlewareManager()
    state.middleware_manager.register_data_middleware(counter)

    # 2. Publish Tasks
    print("Publishing tasks to Redis...")
    run_id = uuid7()
    
    # Batch publish optimization? RedisQueueBackend usually single publish
    start_pub = time.time()
    for i in range(task_count):
        t = TaskModel(
            account=f"user_{i}",
            platform="test_platform",
            module=["demo_spider"],
            run_id=run_id
        )
        await state.mq.publish_task(t)
    
    print(f"Tasks published in {time.time() - start_pub:.2f}s. Starting Worker...")

    # 3. Run Worker
    worker = ConcurrentWorker(worker_id="real-worker", concurrency=workers)
    worker_task = asyncio.create_task(worker.start())
    
    counter.start_time = time.time()
    
    # Wait for completion
    try:
        await asyncio.wait_for(counter.event.wait(), timeout=60.0)
    except asyncio.TimeoutError:
        print(f"Benchmark Timeout! Processed {counter.count}/{task_count}")
    
    duration = counter.end_time - counter.start_time
    
    # Stop Worker
    worker.running = False
    await asyncio.sleep(0.5) # Allow loops to exit
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
        
    await state.mq.close()

    # Results
    print(f"\n--- Results ---")
    print(f"Configuration: Tasks={task_count}, Concurrency={workers}, NetworkDelay=50ms")
    print(f"Total Processed: {counter.count}")
    print(f"Time Taken: {duration:.4f} seconds")
    if duration > 0:
        tps = counter.count / duration
        print(f"Throughput: {tps:.2f} items/second")
        print(f"Expected Max (Theoretical): {workers * (1/0.05):.2f} items/second (approx)")

if __name__ == "__main__":
    count = 500
    workers = 20
    if len(sys.argv) > 1:
        count = int(sys.argv[1])
    if len(sys.argv) > 2:
        workers = int(sys.argv[2])
        
    asyncio.run(run_benchmark(count, workers))
