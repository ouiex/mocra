import logging
from typing import List
from common.models.message import TaskModel, Request
from common.interfaces.processor import BaseProcessor, ProcessorContext, ProcessorResult, ProcessorResultType
from common.state import get_state

logger = logging.getLogger(__name__)

class TaskModelProcessor(BaseProcessor[TaskModel, List[Request]]):
    @property
    def name(self) -> str:
        return "TaskModelProcessor"

    async def process(self, input_item: TaskModel, context: ProcessorContext) -> ProcessorResult[List[Request]]:
        state = get_state()
        if not state.module_manager:
            return ProcessorResult.fatal(Exception("ModuleManager not initialized"))

        # In TaskModel, 'module' can be a list or single string. 
        # For simplicity, we assume if it's a list we take the first one or iterate.
        # But usually a task targets a specific platform, and we find modules for that platform.
        # Here we assume input_item.module contains the target module name(s).
        
        target_modules = input_item.module if input_item.module else []
        if not target_modules:
             # If no module specified, maybe infer from platform? 
             # For Mock, we expect it to be set.
             return ProcessorResult.fatal(Exception("No module specified in task"))

        generated_requests = []
        
        for mod_name in target_modules:
            module = state.module_manager.get_module(mod_name)
            if not module:
                logger.warning(f"Module {mod_name} not found, skipping.")
                continue
                
            logger.info(f"Generating requests using module: {mod_name}")
            try:
                async for req in module.generate(input_item):
                    generated_requests.append(req)
            except Exception as e:
                logger.error(f"Error generating requests in module {mod_name}: {e}")
                return ProcessorResult.retry(e)

        if not generated_requests:
            logger.warning("No requests generated.")
            
        return ProcessorResult.success(generated_requests)
