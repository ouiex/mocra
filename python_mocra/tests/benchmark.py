import asyncio
import time
import logging
import sys
from uuid6 import uuid7
from datetime import datetime

# Add project root to path
import os
sys.path.append(os.getcwd())

from common.state import get_state
from common.models.message import TaskModel, Request
from common.models.response import Response
from common.models.data import Data
from common.models.cookies import Cookies
from common.models.entity import AccountModel, PlatformModel
from engine.worker import UnifiedWorker
from mq.memory_backend import MemoryQueueBackend
from engine.components.module_manager import ModuleManager
from engine.components.middleware_manager import MiddlewareManager
from engine.task.task import Task
from engine.task.module import Module

# Configure Logging - Reduce noise for benchmark
logging.basicConfig(level=logging.WARNING)

# Mocking the actual network call to isolate engine performance
class MockDownloaderManager:
    async def start(self): pass
    async def close(self): pass
    async def download(self, request: Request) -> Response:
        # Simulate slight network delay if needed, but for pure engine throughput we keep it 0
        # await asyncio.sleep(0.01) 
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

async def run_benchmark(task_count: int = 1000):
    print(f"Starting Benchmark with {task_count} tasks...")
    
    # 1. Setup State
    state = get_state()
    state.mq = MemoryQueueBackend()
    state.downloader = MockDownloaderManager()
    
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
            self.event = asyncio.Event()
            self.target = 0

        async def handle_data(self, data: Data) -> Data:
            self.count += 1
            if self.count >= self.target:
                self.end_time = time.time()
                self.event.set()
            return data
            
        async def store_data(self, data: Data) -> None:
            pass

    counter = CounterMiddleware()
    counter.target = task_count # Assuming 1 task produces at least 1 data item (DemoSpider produces 1)
    
    state.middleware_manager = MiddlewareManager()
    state.middleware_manager.register_data_middleware(counter)

    # 2. Publish Tasks
    print("Publishing tasks...")
    run_id = uuid7()
    tasks = []
    for i in range(task_count):
        tasks.append(TaskModel(
            account=f"user_{i}",
            platform="test_platform",
            module=["demo_spider"],
            run_id=run_id
        ))
    
    # Batch publish if possible, or loop
    for t in tasks:
        await state.mq.publish_task(t)

    print("Tasks published. Starting Worker...")
    
    # 3. Run Worker
    worker = UnifiedWorker(worker_id="benchmark-worker")
    # Reduce interval to process faster
    worker.interval = 0.001 
    
    worker_task = asyncio.create_task(worker.start())
    
    counter.start_time = time.time()
    
    # Wait for completion
    try:
        await asyncio.wait_for(counter.event.wait(), timeout=60.0)
    except asyncio.TimeoutError:
        print(f"Benchmark Timeout! Processed {counter.count}/{task_count}")
    
    duration = counter.end_time - counter.start_time
    
    # Stop Worker
    await worker.stop()
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    # Results
    print(f"\n--- Results ---")
    print(f"Total Tasks: {task_count}")
    print(f"Processed: {counter.count}")
    print(f"Time Taken: {duration:.4f} seconds")
    if duration > 0:
        print(f"Throughput: {counter.count / duration:.2f} items/second")

if __name__ == "__main__":
    count = 1000
    if len(sys.argv) > 1:
        count = int(sys.argv[1])
    asyncio.run(run_benchmark(count))
