from typing import List, Optional, Any, Dict
from uuid import UUID
import asyncio
from pydantic import BaseModel

from common.models.entity import AccountModel, PlatformModel
from common.models.message import Request
from engine.task.module import Module

class Task:
    """
    Enhanced Task Structure.
    Contains complete configuration, modules, and execution context.
    """
    def __init__(self,
                 account: AccountModel,
                 platform: PlatformModel,
                 modules: List[Module],
                 metadata: Dict[str, Any],
                 run_id: UUID,
                 prefix_request: UUID,
                 login_info: Any = None):
        self.account = account
        self.platform = platform
        self.modules = modules
        self.metadata = metadata
        self.run_id = run_id
        self.prefix_request = prefix_request
        self.login_info = login_info

    @property
    def id(self) -> str:
        return f"{self.account.name}-{self.platform.name}"

    def get_module_names(self) -> List[str]:
        return [m.id for m in self.modules]
    
    def is_empty(self) -> bool:
        return len(self.modules) == 0

    async def build_requests(self, original_task_model: Any) -> List[Request]:
        """
        Concurrently execute generation for all modules.
        """
        all_requests = []
        
        # In Python we don't have join_all generally available for generators easily unless we wrap them.
        # We iterate sequentially or use asyncio.gather on wrapped coroutines.
        # Since module.generate is an AsyncGenerator, we consume it.
        
        tasks = []
        for module in self.modules:
            tasks.append(self._collect_module_requests(module, original_task_model))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for res in results:
            if isinstance(res, list):
                all_requests.extend(res)
            elif isinstance(res, Exception):
                # Log error but don't crash everything? 
                # Rust returns Err(e) to caller.
                raise res
                
        return all_requests

    async def _collect_module_requests(self, module: Module, task_model: Any) -> List[Request]:
        reqs = []
        async for req in module.generate(task_model):
            reqs.append(req)
        return reqs
