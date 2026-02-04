import asyncio
import time
import logging
import sys
import os
from uuid6 import uuid7

# Add project root to path
sys.path.append(os.getcwd())

from common.state import get_state
from common.models.message import TaskModel, Request
from common.models.response import Response
from common.models.cookies import Cookies
from engine.worker import UnifiedWorker
from mq.memory_backend import MemoryQueueBackend
from engine.components.module_manager import ModuleManager
from engine.components.middleware_manager import MiddlewareManager
from engine.task.task import Task
from engine.task.module import Module
from common.models.entity import AccountModel, PlatformModel
from common.interfaces.middleware import BaseDownloadMiddleware

# Configure Logging
logging.basicConfig(level=logging.WARNING)

class MockDownloaderManager:
    async def start(self):
        pass

    async def close(self):
        pass

    async def download(self, request: Request) -> Response:
        return Response(
            id=request.id,
            platform=request.platform,
            account=request.account,
            module=request.module,
            status_code=200,
            cookies=Cookies(),
            content=b"OK",
            headers=[("Content-Type", "text/plain")],
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

class CounterDownloadMiddleware(BaseDownloadMiddleware):
    name = "counter_download_middleware"

    def __init__(self, target: int):
        self.count = 0
        self.target = target
        self.start_time = 0.0
        self.end_time = 0.0
        self.event = asyncio.Event()

    async def after_response(self, response: Response) -> Response:
        self.count += 1
        if self.count >= self.target:
            self.end_time = time.time()
            self.event.set()
        return response

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

async def run_benchmark(task_count: int = 2000):
    print(f"Starting Pure Mock Full-Engine Benchmark with {task_count} tasks...")

    os.environ.setdefault("MOCK_DEV_URL", "mock://local/test")
    os.environ["MOCK_DEV_COUNT"] = str(task_count)

    state = get_state()
    state.mq = MemoryQueueBackend()
    state.downloader = MockDownloaderManager()

    state.module_manager = ModuleManager()
    from modules.mock_dev import MockDevSpider
    state.module_manager._modules["mock.dev"] = MockDevSpider()

    state.task_manager = BenchmarkTaskManager(state)

    counter = CounterDownloadMiddleware(task_count)
    state.middleware_manager = MiddlewareManager()
    state.middleware_manager.register_download_middleware(counter)

    run_id = uuid7()
    task = TaskModel(
        account="benchmark",
        platform="mock",
        module=["mock.dev"],
        run_id=run_id,
    )

    await state.mq.publish_task(task)

    worker = UnifiedWorker(worker_id="benchmark-worker")
    worker.interval = 0.001

    worker_task = asyncio.create_task(worker.start())
    counter.start_time = time.time()

    try:
        await asyncio.wait_for(counter.event.wait(), timeout=60.0)
    except asyncio.TimeoutError:
        print(f"Benchmark Timeout! Processed {counter.count}/{task_count}")

    duration = counter.end_time - counter.start_time

    await worker.stop()
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    print("\n--- Results ---")
    print(f"Total Requests: {task_count}")
    print(f"Processed: {counter.count}")
    print(f"Time Taken: {duration:.4f} seconds")
    if duration > 0:
        print(f"Throughput: {counter.count / duration:.2f} items/second")

if __name__ == "__main__":
    count = 2000
    if len(sys.argv) > 1:
        count = int(sys.argv[1])
    asyncio.run(run_benchmark(count))
