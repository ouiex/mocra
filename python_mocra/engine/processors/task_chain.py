import logging
import time
from typing import List, Optional
from common.models.message import TaskModel, Request
from common.interfaces.processor import BaseProcessor, ProcessorContext, ProcessorResult, ProcessorResultType, RetryPolicy
from common.state import get_state
from engine.core.events import (
    EventTaskReceived,
    EventTaskStarted,
    EventTaskCompleted,
    EventTaskFailed,
    TaskEventPayload,
    EventRequestPublish,
    RequestEventPayload,
)
from common.status_tracker import ErrorDecision

logger = logging.getLogger(__name__)

class TaskModelProcessor(BaseProcessor[TaskModel, List[Request]]):
    @property
    def name(self) -> str:
        return "TaskModelProcessor"

    async def process(self, input_item: TaskModel, context: ProcessorContext) -> ProcessorResult[List[Request]]:
        state = get_state()
        status = state.status_tracker
        if state.event_bus:
            state.event_bus.publish(
                EventTaskReceived(
                    payload=TaskEventPayload(
                        task_id=input_item.task_id,
                        account=input_item.account,
                        platform=input_item.platform,
                        run_id=str(input_item.run_id),
                        modules=input_item.module,
                    )
                )
            )
        
        # 1. Status Check
        if status:
            decision = await status.should_task_continue(input_item.task_id)
            if decision == ErrorDecision.TERMINATE:
                 logger.error(f"[Task] Task {input_item.task_id} terminated by tracker")
                 return ProcessorResult.fatal(Exception(f"Task {input_item.task_id} terminated"))
        
        # 2. Module Load & Generation
        if state.event_bus:
            state.event_bus.publish(
                EventTaskStarted(
                    payload=TaskEventPayload(
                        task_id=input_item.task_id,
                        account=input_item.account,
                        platform=input_item.platform,
                        run_id=str(input_item.run_id),
                        modules=input_item.module,
                    )
                )
            )
        if not state.task_manager:
            return ProcessorResult.fatal(Exception("TaskManager not initialized"))

        try:
             # Use TaskManager to construct the Task entity (validates modules, config, etc.)
             task_obj = await state.task_manager.load_with_model(input_item)
        except Exception as e:
             logger.error(f"[Task] Failed to load task: {e}")
             if state.event_bus:
                 state.event_bus.publish(
                     EventTaskFailed(
                         payload=TaskEventPayload(
                             task_id=input_item.task_id,
                             account=input_item.account,
                             platform=input_item.platform,
                             run_id=str(input_item.run_id),
                             modules=input_item.module,
                         ),
                         error=str(e),
                     )
                 )
             return ProcessorResult.fatal(e)
             
        generated_requests = []
        
        # Iterate over configured modules in the task
        for module in task_obj.modules:
            # Check module status
            # Construct ID manually or use property
            mod_id = f"{module.account.name}-{module.platform.name}-{module.name}"
            
            if status:
                mod_decision = await status.should_module_continue(mod_id)
                if mod_decision == ErrorDecision.TERMINATE:
                     logger.warning(f"[Task] Module {module.name} terminated, skipping generation")
                     continue

            logger.info(f"[Task] Generating for {module.name}")
            try:
                # module.generate now handles context enrichment (run_id, account, etc.)
                async for req in module.generate(input_item):
                    generated_requests.append(req)
            except Exception as e:
                logger.error(f"[Task] Generation failed for {module.name}: {e}")
        
        if not generated_requests:
            logger.warning("[Task] No requests generated")

        if state.event_bus:
            state.event_bus.publish(
                EventTaskCompleted(
                    payload=TaskEventPayload(
                        task_id=input_item.task_id,
                        account=input_item.account,
                        platform=input_item.platform,
                        run_id=str(input_item.run_id),
                        modules=input_item.module,
                    ),
                    metrics={"request_count": len(generated_requests)},
                )
            )
            
        return ProcessorResult.success(generated_requests)

class DeduplicationProcessor(BaseProcessor[List[Request], List[Request]]):
    @property
    def name(self) -> str:
        return "DeduplicationProcessor"
        
    async def process(self, input_item: List[Request], context: ProcessorContext) -> ProcessorResult[List[Request]]:
        state = get_state()
        
        # If deduplication service not enabled or input empty, pass through
        if not state.deduplicator or not input_item:
            return ProcessorResult.success(input_item)
            
        # Extract hashes
        hashes = [req.hash() for req in input_item]
        
        try:
            # check_and_set_batch returns list of booleans: True if NEW, False if DUPLICATE
            is_new_list = await state.deduplicator.check_and_set_batch(hashes)
            
            unique_requests = []
            for req, is_new in zip(input_item, is_new_list):
                if is_new:
                    unique_requests.append(req)
                else:
                    logger.debug(f"[Dedup] Duplicate found: {req.url}")
            
            filtered_count = len(input_item) - len(unique_requests)
            if filtered_count > 0:
                logger.info(f"[Dedup] Filtered {filtered_count} duplicates")
                
            return ProcessorResult.success(unique_requests)
            
        except Exception as e:
            logger.error(f"[Dedup] Error checking duplicates: {e}")
            # Fail open: if redis error, process requests anyway
            return ProcessorResult.success(input_item)

class RequestPublishProcessor(BaseProcessor[List[Request], int]):
    """
    Publishes a list of requests to the MQ. Returns count of published.
    """
    @property
    def name(self) -> str:
        return "RequestPublishProcessor"

    async def process(self, input_item: List[Request], context: ProcessorContext) -> ProcessorResult[int]:
        state = get_state()
        if not state.mq:
             return ProcessorResult.success(0)
             
        count = 0
        for req in input_item:
            try:
                # We might want to batch this in valid MQ implementations
                await state.mq.publish_request(req)
                count += 1
                if state.event_bus:
                    state.event_bus.publish(
                        EventRequestPublish(
                            payload=RequestEventPayload(
                                url=req.url,
                                method=req.method,
                                request_id=str(req.id),
                                meta=req.meta.model_dump(),
                                account=req.account,
                                platform=req.platform,
                                module=req.module,
                            )
                        )
                    )
            except Exception as e:
                logger.error(f"[Task] Failed to publish request {req.url}: {e}")
                
        logger.info(f"[Task] Published {count} requests")
        return ProcessorResult.success(count)
