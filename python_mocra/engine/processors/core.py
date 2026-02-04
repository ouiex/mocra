import asyncio
import logging
from typing import Optional
from common.models.message import TaskModel, Request, Response
from common.interfaces.processor import BaseProcessor, ProcessorContext, ProcessorResult

logger = logging.getLogger(__name__)

class TaskModelProcessor(BaseProcessor[TaskModel, list[Request]]):
    @property
    def name(self) -> str:
        return "TaskModelProcessor"

    async def process(self, input_item: TaskModel, context: ProcessorContext) -> ProcessorResult[list[Request]]:
        logger.info(f"Processing Task: {input_item.task_id}")
        
        # 1. Load Module (Mock)
        # 2. Generate Requests
        
        # Mock request generation
        req = Request(
            platform=input_item.platform,
            account=input_item.account,
            module="test_module",
            url="https://example.com",
            run_id=input_item.run_id
        )
        
        return ProcessorResult.success([req])

class MockDownloadProcessor(BaseProcessor[Request, Response]):
    @property
    def name(self) -> str:
        return "MockDownloadProcessor"

    async def process(self, input_item: Request, context: ProcessorContext) -> ProcessorResult[Response]:
        logger.info(f"Downloading: {input_item.url}")
        await asyncio.sleep(0.1) # Simulate network IO
        
        resp = Response(
            platform=input_item.platform,
            account=input_item.account,
            module=input_item.module,
            status_code=200,
            headers={"Content-Type": "text/html"},
            content=b"hello world",
            context=input_item.context,
            run_id=input_item.run_id,
            prefix_request=input_item.id
        )
        return ProcessorResult.success(resp)
