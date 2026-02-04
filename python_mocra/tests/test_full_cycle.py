import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid6 import uuid7
from datetime import datetime

from common.state import get_state
from common.models.message import TaskModel, Request, Response
from common.models.data import Data
from engine.worker import UnifiedWorker
from mq.memory_backend import MemoryQueueBackend
from engine.components.module_manager import ModuleManager

# Mocking the actual network call
class MockDownloaderManager:
    async def start(self): pass
    async def close(self): pass
    async def download(self, request: Request) -> Response:
        from common.models.headers import Headers
        from common.models.cookies import Cookies
        from common.models.meta import MetaData

        return Response(
            id=uuid7(),
            platform=request.platform,
            account=request.account,
            module=request.module,
            status_code=200,
            url=request.url,
            content=b'{"slide": "slideshow", "title": "Sample Slide Show"}',
            headers=[("Content-Type", "application/json")],
            cookies=Cookies(),
            metadata=MetaData(),
            task_retry_times=0,
            download_middleware=[],
            data_middleware=[],
            task_finished=False,
            run_id=request.run_id,
            context=request.context,
            prefix_request=request.id
        )

@pytest.mark.asyncio
async def test_full_cycle():
    """
    Tests the full lifecycle of a task:
    Task -> Request -> Response -> Data
    """
    # 1. Setup State
    state = get_state()
    # Use Memory Backend for testing
    state.mq = MemoryQueueBackend()
    
    # Mock Downloader
    state.downloader = MockDownloaderManager()
    
    # Load Modules
    state.module_manager = ModuleManager()
    # Ensure demo_spider is loaded. 
    # Depending on how load_modules works, we might need to point it to the directory
    # or manually register connection.
    # state.module_manager.load_modules() 
    # Let's manually register the class if possible or ensure path is correct.
    # Assuming 'modules.demo.DemoSpider' is discoverable or we import it.
    from modules.demo import DemoSpider
    state.module_manager._modules["demo_spider"] = DemoSpider()

    from engine.task.task_manager import TaskManager
    state.task_manager = TaskManager()

    # Mock TaskFactory to bypass DB
    from uuid6 import uuid7
    from engine.task.task import Task
    from common.models.entity import AccountModel, PlatformModel
    from engine.task.module import Module

    class MockTaskFactory:
        def __init__(self, state): pass

        async def load_with_model(self, task_model):
            # Manual construction of Task object
            modules = []
            # We need to find the module instance to add to the task
            for mod_name in task_model.module:
                # Look up in module manager
                base_mod = state.module_manager.get_module(mod_name)
                if base_mod:
                    # minimal module instance
                    mod_inst = Module(
                        module_instance=base_mod,
                        config={},
                        account=AccountModel(name=task_model.account),
                        platform=PlatformModel(name=task_model.platform),
                        run_id=task_model.run_id,
                        prefix_request=uuid7()
                    )
                    modules.append(mod_inst)

            return Task(
                account=AccountModel(name=task_model.account),
                platform=PlatformModel(name=task_model.platform),
                modules=modules,
                metadata=task_model.model_dump(),
                run_id=task_model.run_id,
                prefix_request=uuid7()
            )
        
        async def load_parser_model(self, parser_model): pass
        async def load_error_model(self, error_model): pass
    
    state.task_manager.factory = MockTaskFactory(state)
    
    # 2. Publish initial Task
    run_id = uuid7()
    task = TaskModel(
        account="test_user",
        platform="test_platform",
        module=["demo_spider"],
        run_id=run_id
    )
    
    await state.mq.publish_task(task)
    
    # 3. specific validation lists
    # We will peek into the memory queue or capture outputs via mocks if we want strict assertion.
    # But checking if Data is produced is a good end-to-end check.
    
    # We need to capture the Data produced.
    # The default DataMiddleware writes to file. We can mock middleware manager.
    
    received_data = []

    class MockDataMiddleware:
        name = "MockDataMiddleware"
        async def handle_data(self, data: Data) -> Data:
            received_data.append(data)
            return data
        async def store_data(self, data: Data) -> None:
            pass
            
    from engine.components.middleware_manager import MiddlewareManager
    state.middleware_manager = MiddlewareManager()
    state.middleware_manager.register_data_middleware(MockDataMiddleware())

    # 4. Run Worker
    # We act as a worker processing messages for a short duration
    worker = UnifiedWorker(worker_id="test-worker")
    
    # Start worker in background
    worker_task = asyncio.create_task(worker.start())
    
    # Wait for processing
    # The cycle:
    # T0: Task in Queue
    # T1: Worker picks Task -> Generates Request -> Puts in Request Queue
    # T2: Worker picks Request -> Downloads (Mock) -> Puts in Response Queue
    # T3: Worker picks Response -> Parses -> Generates Data -> Middleware captures
    
    # We poll until we see data or timeout
    try:
        for _ in range(20): # 2 seconds max
            if received_data:
                break
            await asyncio.sleep(0.1)
    finally:
        await worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    # 5. Assertions
    assert len(received_data) > 0
    data = received_data[0]
    assert data.module == "demo_spider"
    assert data.account == "test_user"
    assert "slide" in data.content or "slide" in str(data.content)
