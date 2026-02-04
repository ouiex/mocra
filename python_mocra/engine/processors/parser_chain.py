import logging
import time
from typing import Optional, List, Tuple
from common.models.message import Response, Request, ParserData, ParserTaskModel
from common.models.data import Data
from common.interfaces.module import ModuleNodeTrait
from common.interfaces.processor import BaseProcessor, ProcessorContext, ProcessorResult, ProcessorResultType, RetryPolicy
from common.state import get_state
from common.status_tracker import ErrorDecision

logger = logging.getLogger(__name__)

# -- 0. ResponseReloadProcessor --
class ResponseReloadProcessor(BaseProcessor[Response, Response]):
    """Reloads response content from blob storage if offloaded."""

    @property
    def name(self) -> str:
        return "ResponseReloadProcessor"

    async def process(self, input_item: Response, context: ProcessorContext) -> ProcessorResult[Response]:
        state = get_state()
        if state.blob_storage and input_item.storage_path and not input_item.content:
            try:
                await input_item.reload(state.blob_storage)
            except Exception as e:
                logger.error(f"[Reload] Failed to reload response content: {e}")
                return ProcessorResult.retry(e)
        return ProcessorResult.success(input_item)

# -- 1. ResponseModuleProcessor --
class ResponseModuleProcessor(BaseProcessor[Response, Response]):
    """
    Validates module availability and status before parsing.
    """
    @property
    def name(self) -> str:
        return "ResponseModuleProcessor"

    async def process(self, input_item: Response, context: ProcessorContext) -> ProcessorResult[Response]:
        state = get_state()
        status = state.status_tracker
        
        # 0. Check tracker
        if status:
             # Task check
             task_id = f"{input_item.account}-{input_item.platform}"
             t_decision = await status.should_task_continue(task_id)
             if t_decision == ErrorDecision.TERMINATE:
                  return ProcessorResult.fatal(Exception(f"Task {task_id} terminated"))

             # Module check
             m_decision = await status.should_module_continue(input_item.module)
             if m_decision == ErrorDecision.TERMINATE:
                  return ProcessorResult.fatal(Exception(f"Module {input_item.module} terminated"))
        
        # 1. Check Module existence
        if not state.module_manager or not state.module_manager.get_module(input_item.module):
             return ProcessorResult.fatal(Exception(f"Module {input_item.module} not found"))
             
        return ProcessorResult.success(input_item)

# -- 2. ResponseParserProcessor --
class ResponseParserProcessor(BaseProcessor[Response, ParserData]):
    """
    Executes the module parser.
    """
    @property
    def name(self) -> str:
        return "ResponseParserProcessor"

    async def process(self, input_item: Response, context: ProcessorContext) -> ProcessorResult[ParserData]:
        state = get_state()
        module = state.module_manager.get_module(input_item.module)
        
        try:
            # Rust invokes: module.parser(response)
            # Python: module.dispatch(response) (supports callbacks)
            # Or directly module.parser if it's a ModuleNodeTrait
            
            # We assume module is the Module wrapper which delegates to Node
            # But module_manager returns the user module instance (ModuleNodeTrait).
            # We need to wrap it if it doesn't support dispatch.
            if not hasattr(module, 'dispatch'):
                from engine.task.module import Module
                from common.models.entity import AccountModel, PlatformModel
                from uuid import UUID
                # Reconstruct wrapper
                # Ideally we should use TaskManager to get fully configured module, but we lack Task context here easily.
                # For parsing, minimal context is usually enough.
                wrapper = Module(
                    module_instance=module,
                    config={}, # We don't have config here easily unless we fetch from TaskStore or assume defaults
                    account=AccountModel(name=input_item.account),
                    platform=PlatformModel(name=input_item.platform),
                    run_id=input_item.run_id,
                    prefix_request=input_item.prefix_request,
                )
                result = await wrapper.dispatch(input_item)
            else:
                result = await module.dispatch(input_item)
                
            return ProcessorResult.success(result)
        except Exception as e:
            url = input_item.metadata.get_trait_config('url') or 'unknown'
            logger.error(f"[Parser] Failed parsing {url}: {e}")
            return ProcessorResult.retry(e)

# -- 3. ResultRoutingProcessor --
class ResultRoutingProcessor(BaseProcessor[ParserData, ParserData]):
    """
    Routes parsed results: 
    - Data -> Middleware -> Storage
    - ParserTask -> Generate Requests -> MQ
    - ErrorTask -> MQ
    """
    @property
    def name(self) -> str:
        return "ResultRoutingProcessor"

    async def process(self, input_item: ParserData, context: ProcessorContext) -> ProcessorResult[ParserData]:
        state = get_state()
        
        # 1. Process Data
        if input_item.data:
            # For Python simple implementation, we iterate middlewares here or in manager
            if state.middleware_manager:
                 for data in input_item.data:
                      await self._process_data(state, data)

        # 2. Process ParserTask (Generate Requests)
        if input_item.parser_task:
            task = input_item.parser_task
            # Get target module
            target_module_name = task.account_task.module[0] if task.account_task.module else None
            
            # Simple logic: If local module available, generate here. Else publish task.
            # Assuming we can always generate locally if module loaded
            module = state.module_manager.get_module(target_module_name)
            
            if module:
                 try:
                     # Generate requests
                     # We need to adapt generate signature to support ParserTaskModel
                     # Usually generate takes (config, params, login_info)
                     # Module wrapper should handle this adaptation
                     async for req in module.generate_from_task(task):
                         if state.mq:
                             await state.mq.publish_request(req)
                 except Exception as e:
                     logger.error(f"[Parser] Failed to generate requests from task: {e}")
            else:
                # Module not local? Publish task to be picked up by someone else?
                # Or just error out. 
                logger.warning(f"[Parser] Target module {target_module_name} not found, publishing task to MQ")
                if state.mq:
                    await state.mq.publish_parser_task(task)

        # 3. Process ErrorTask
        if input_item.error_task:
            if state.mq:
                # Publish error task
                # Not fully implemented in MQ yet?
                pass

        return ProcessorResult.success(input_item)

    async def _process_data(self, state, data: Data):
        # Apply middlewares
        if state.middleware_manager:
            for mw in state.middleware_manager.data_middlewares:
                try:
                    await mw.handle_data(data)
                except Exception as e:
                    logger.error(f"[DataMiddleware] {mw.name} failed: {e}")
