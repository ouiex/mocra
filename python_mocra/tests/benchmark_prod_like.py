import asyncio
import time
import logging
import os
import sys
from uuid6 import uuid7

# Add project root to path
sys.path.append(os.getcwd())

from common.state import get_state
from common.models.message import TaskModel, Request
from common.models.response import Response
from common.models.cookies import Cookies
from common.models.data import Data
from engine.worker import UnifiedWorker
from engine.components.module_manager import ModuleManager
from engine.components.middleware_manager import MiddlewareManager
from engine.task.task_manager import TaskManager
from common.interfaces.middleware import BaseDownloadMiddleware
from common.models.db import Account, Platform, Module, RelAccountPlatform, RelModuleAccount, RelModulePlatform
from sqlalchemy import select

logging.basicConfig(level=logging.WARNING)
logging.getLogger("engine.worker").setLevel(logging.WARNING)
logging.getLogger("engine.processors.task_chain").setLevel(logging.WARNING)

class HttpxDownloaderManager:
    def __init__(self, timeout: float = 10.0):
        import httpx

        self._client = httpx.AsyncClient(timeout=timeout, http2=True)

    async def start(self):
        pass

    async def close(self):
        await self._client.aclose()

    async def download(self, request: Request) -> Response:
        resp = await self._client.get(request.url)
        headers = [(k, v) for k, v in resp.headers.items()]
        return Response(
            id=request.id,
            platform=request.platform,
            account=request.account,
            module=request.module,
            status_code=resp.status_code,
            cookies=Cookies(),
            content=resp.content,
            headers=headers,
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
        if self.count % 500 == 0:
            print(f"Processed: {self.count}/{self.target}")
        if self.count >= self.target:
            self.end_time = time.time()
            self.event.set()
        return response

async def seed_database(state):
    if not state.db:
        raise RuntimeError("Database not initialized")
    async with state.db.session_factory() as session:
        existing_account = (await session.execute(
            select(Account).where(Account.name == "benchmark")
        )).scalar_one_or_none()
        if existing_account:
            account_id = existing_account.id
        else:
            account = Account(name="benchmark", enabled=True, priority=5, modules=[])
            session.add(account)
            await session.flush()
            account_id = account.id

        existing_platform = (await session.execute(
            select(Platform).where(Platform.name == "mock")
        )).scalar_one_or_none()
        if existing_platform:
            platform_id = existing_platform.id
        else:
            platform = Platform(name="mock", description="Mock Platform", enabled=True)
            session.add(platform)
            await session.flush()
            platform_id = platform.id

        existing_module = (await session.execute(
            select(Module).where(Module.name == "mock.dev")
        )).scalar_one_or_none()
        if existing_module:
            module_id = existing_module.id
        else:
            module = Module(name="mock.dev", version=1, priority=5, enabled=True)
            session.add(module)
            await session.flush()
            module_id = module.id

        rel_ap_exists = (await session.execute(
            select(RelAccountPlatform).where(
                RelAccountPlatform.account_id == account_id,
                RelAccountPlatform.platform_id == platform_id,
            )
        )).scalar_one_or_none()
        if not rel_ap_exists:
            session.add(RelAccountPlatform(account_id=account_id, platform_id=platform_id, enabled=True, config={}))

        rel_ma_exists = (await session.execute(
            select(RelModuleAccount).where(
                RelModuleAccount.module_id == module_id,
                RelModuleAccount.account_id == account_id,
            )
        )).scalar_one_or_none()
        if not rel_ma_exists:
            session.add(RelModuleAccount(module_id=module_id, account_id=account_id, enabled=True, priority=5, config={}))

        rel_mp_exists = (await session.execute(
            select(RelModulePlatform).where(
                RelModulePlatform.module_id == module_id,
                RelModulePlatform.platform_id == platform_id,
            )
        )).scalar_one_or_none()
        if not rel_mp_exists:
            session.add(RelModulePlatform(module_id=module_id, platform_id=platform_id, enabled=True, priority=5, config={}))

        await session.commit()

async def run_benchmark(task_count: int = 2000):
    print(f"Starting Prod-Like Benchmark with {task_count} tasks...")

    os.environ.setdefault("MOCK_DEV_URL", "mock://local/test")
    os.environ["MOCK_DEV_COUNT"] = str(task_count)

    state = get_state()

    # Use local sqlite for DB, but keep Redis-based services to simulate production
    default_db_path = os.path.join("tests", "tmp", "prod_like.sqlite")
    os.makedirs(os.path.dirname(default_db_path), exist_ok=True)
    db_url = os.environ.get("DB_URL", f"sqlite+aiosqlite:///{default_db_path}")
    state.config.database.url = db_url

    # Force Redis MQ backend for prod-like flow
    state.config.mq_backend = "redis"

    await state.init()

    print(f"MQ backend: {type(state.mq).__name__}")

    if state.mq and hasattr(state.mq, "_redis"):
        await state.mq._redis.delete("queue:task", "queue:request", "queue:response", "queue:parser_task")

    # Seed DB if empty (best-effort)
    try:
        await seed_database(state)
    except Exception as exc:
        logging.warning(f"Seed database skipped or failed: {exc}")

    use_http = os.environ.get("PROD_LIKE_USE_HTTP", "1") != "0"
    if use_http:
        state.downloader = HttpxDownloaderManager(timeout=10.0)
    else:
        state.downloader = MockDownloaderManager()

    state.module_manager = ModuleManager()
    from modules.mock_dev import MockDevSpider
    state.module_manager._modules["mock.dev"] = MockDevSpider()

    state.task_manager = TaskManager()

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

    if not state.mq:
        raise RuntimeError("MQ backend not initialized")

    await state.mq.publish_task(task)

    class ConcurrentWorker(UnifiedWorker):
        async def start(self):
            self.running = True

            async def task_loop():
                while self.running:
                    await self._process_task()

            async def request_loop():
                while self.running:
                    await self._process_request()

            async def response_loop():
                while self.running:
                    await self._process_response()

            await asyncio.gather(
                task_loop(),
                request_loop(),
                response_loop(),
            )

    worker = ConcurrentWorker(worker_id="prod-like-worker")

    worker_task = asyncio.create_task(worker.start())
    counter.start_time = time.time()

    try:
        await asyncio.wait_for(counter.event.wait(), timeout=120.0)
    except asyncio.TimeoutError:
        print(f"Benchmark Timeout! Processed {counter.count}/{task_count}")
        if not counter.end_time:
            counter.end_time = time.time()

    duration = counter.end_time - counter.start_time

    await worker.stop()
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    await state.close()

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
