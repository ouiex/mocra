import asyncio
import logging
import signal
from typing import List, Optional
from common.state import get_state
from common.models.message import TaskModel, Request, Response
from engine.processors.task_chain import TaskModelProcessor, RequestPublishProcessor, DeduplicationProcessor
from engine.processors.parser_chain import ResponseReloadProcessor, ResponseModuleProcessor, ResponseParserProcessor, ResultRoutingProcessor
from engine.processors.download_chain import (
    ConfigProcessor, ProxyProcessor, RequestMiddlewareProcessor, 
    DownloadProcessor, ResponseMiddlewareProcessor, ResponseOffloadProcessor, ResponsePublishProcessor
)
from engine.core.pipeline import Pipeline

logger = logging.getLogger(__name__)

class UnifiedWorker:
    """
    A worker that runs all pipelines in a loop (for simple deployments).
    In a real distributed setup, you might run separate workers for Task, Download, and Parse.
    """
    def __init__(self, worker_id: str = "worker-1", interval: float = 0.1):
        self.worker_id = worker_id
        self.interval = interval
        self.running = False
        
        # Pipelines
        
        # 1. Task Chain: TaskModel -> [Requests] -> Publish
        self.task_pipeline = Pipeline([
            TaskModelProcessor(),
            DeduplicationProcessor(),
            RequestPublishProcessor()
        ])
        
        # 2. Download Chain: Request -> Response -> Publish
        self.download_pipeline = Pipeline([
            ConfigProcessor(),
            ProxyProcessor(),
            RequestMiddlewareProcessor(),
            DownloadProcessor(),
            ResponseMiddlewareProcessor(),
            ResponseOffloadProcessor(),
            ResponsePublishProcessor()
        ])
        
        # 3. Parser Chain: Response -> ParserResult -> Route(Data/Req)
        self.parser_pipeline = Pipeline([
            ResponseReloadProcessor(),
            ResponseModuleProcessor(),
            ResponseParserProcessor(),
            ResultRoutingProcessor()
        ])

    async def start(self):
        self.running = True
        logger.info(f"Worker {self.worker_id} started. backend: {type(get_state().mq).__name__}")
        
        # In a real app, these would be concurrent tasks
        while self.running:
            did_work = False
            
            # 1. Consume Tasks -> Produce Requests
            if await self._process_task():
                did_work = True
                
            # 2. Consume Requests -> Download -> Produce Responses
            if await self._process_request():
                did_work = True
                
            # 3. Consume Responses -> Parse -> Store Data / Produce New Items
            if await self._process_response():
                did_work = True
                
            if not did_work:
                await asyncio.sleep(self.interval)

    async def stop(self):
        logger.info(f"Worker {self.worker_id} stopping...")
        self.running = False

    async def _process_task(self) -> bool:
        state = get_state()
        queued_task = await state.mq.consume_task("default", self.worker_id)
        if not queued_task:
            return False
        
        task = queued_task.item
        logger.info(f"[Task] Processing {task.task_id}")
        result = await self.task_pipeline.run(task)
        
        if result.status.value == "success":
            await queued_task.ack()
        else:
            logger.error(f"[Task] Failed: {result.error}")
            await queued_task.nack(str(result.error))
            
        return True

    async def _process_request(self) -> bool:
        state = get_state()
        queued_req = await state.mq.consume_request("default", self.worker_id)
        if not queued_req:
            return False
        
        req = queued_req.item
        logger.info(f"[Down] Processing {req.url}")
        
        # The new Download Chain returns None (since PublishProcessor is terminal in chain)
        # OR returns result. But in Rust chain last element is ResponsePublish -> result is ()
        result = await self.download_pipeline.run(req)
        
        if result.status.value == "success":
             await queued_req.ack()
        else:
            logger.error(f"[Down] Chain Failed: {result.error}")
            await queued_req.nack(str(result.error))
            
        return True

    async def _process_response(self) -> bool:
        state = get_state()
        # Note: We need to cast mq to something that has consume_response or update interface
        # For now assuming simple MemoryBackend or similar that supports it
        if hasattr(state.mq, 'consume_response'):
            queued_resp = await state.mq.consume_response("default", self.worker_id)
        else:
            # Fallback/Fail if backend doesn't support consume_response
            return False
            
        if not queued_resp:
            return False
        
        resp = queued_resp.item
        logger.info(f"[Parse] Processing response from {resp.module}")
        result = await self.parser_pipeline.run(resp)
        
        if result.status.value == "success":
            parsed_res = result.data
            
            # Data and Requests are handled by ResultRoutingProcessor in the chain
            
            # Handle New Tasks (if not handled by processor)
            # ResultRoutingProcessor handles parser_task (singular)
            # If we had a list of tasks, we would handle them here or in processor.
            # Currently ParserData has only singular parser_task which is handled by processor.
            # So we remove the broken loop.
            # for new_task in parsed_res.tasks:
            #    await state.mq.publish_task(new_task)
            
            await queued_resp.ack()
                
        else:
            logger.error(f"[Parse] Failed: {result.error}")
            await queued_resp.nack(str(result.error))
            
        return True
