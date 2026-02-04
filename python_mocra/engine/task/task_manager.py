import logging
from typing import List, Optional, Any
from common.state import get_state
from common.models.message import TaskModel, ParserTaskModel, ErrorTaskModel
from engine.task.task import Task
from engine.task.factory import TaskFactory
from common.interfaces.module import BaseModule

logger = logging.getLogger(__name__)

class TaskManager:
    def __init__(self):
        self.state = get_state()
        self.factory = TaskFactory(self.state)
        # In Rust: module_assembler. Here: we reuse state.module_manager for module registration.

    async def add_module(self, module: BaseModule):
        if self.state.module_manager:
            # We don't have dynamic add method on module_manager yet, usually it scans dir.
            self.state.module_manager._modules[module.name] = module

    async def exists_module(self, name: str) -> bool:
        if self.state.module_manager:
             return self.state.module_manager.get_module(name) is not None
        return False

    async def load_with_model(self, task_model: TaskModel) -> Task:
        return await self.factory.load_with_model(task_model)

    async def load_parser(self, parser_model: ParserTaskModel) -> Task:
        return await self.factory.load_parser_model(parser_model)
        
    async def load_error(self, error_model: ErrorTaskModel) -> Task:
        return await self.factory.load_error_model(error_model)
