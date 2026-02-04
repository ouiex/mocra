from typing import List, Optional, Any, Dict, AsyncGenerator
from uuid import UUID
from pydantic import BaseModel, Field

from common.models.entity import AccountModel, PlatformModel
from common.models.message import Request, Response, TaskModel, ParserTaskModel, ParserData
from common.config import AppConfig 
from common.interfaces.module import ModuleNodeTrait
from common.models.login_info import LoginInfo

class Module:
    """
    Module Entity.
    Represents a specific instance of a crawler module with configuration, 
    account, and platform context.
    Wrapper around the user-defined ModuleNodeTrait.
    """
    def __init__(self, 
                 module_instance: ModuleNodeTrait,
                 config: Any,
                 account: AccountModel,
                 platform: PlatformModel,
                 run_id: UUID,
                 prefix_request: UUID,
                 download_middleware: List[str] = None,
                 data_middleware: List[str] = None,
                 login_info: Optional[LoginInfo] = None):
        
        self.module = module_instance
        self.config = config
        self.account = account
        self.platform = platform
        self.run_id = run_id
        self.prefix_request = prefix_request 
        self.login_info = login_info
        
        self.download_middleware = download_middleware or []
        self.data_middleware = data_middleware or []
        
        self.error_times = 0
        self.finished = False
        self.locker = False
        self.locker_ttl = 0
        
        self.pending_ctx = None

    @property
    def name(self) -> str:
        # Handle both property and method implementation of name
        if hasattr(self.module, 'name'):
            val = self.module.name
            if callable(val):
                return val()
            return val
        return "unknown"
    
    @property
    def id(self) -> str:
        return self.name

    async def generate(self, task_model: TaskModel) -> AsyncGenerator[Request, None]:
        """
        Generates requests from a TaskModel (initial task).
        """
        # TaskModel doesn't have params, so we pass empty dict or extract from somewhere if needed.
        # Assuming empty params for initial task.
        params = {}
        
        async for req in self._generate_internal(params):
            yield req

    async def generate_from_task(self, task: ParserTaskModel) -> AsyncGenerator[Request, None]:
        """
        Generates requests from a ParserTaskModel (continuation task).
        """
        params = task.metadata if task.metadata else {}
        
        # Use task context if available
        # We might want to temporarily override pending_ctx here
        old_ctx = self.pending_ctx
        if task.context:
            self.pending_ctx = task.context
            
        try:
            async for req in self._generate_internal(params):
                # Ensure we propagate the correct run_id and prefix if from task
                req.run_id = task.run_id
                req.prefix_request = task.prefix_request
                if task.context:
                    req.context = task.context
                yield req
        finally:
             self.pending_ctx = old_ctx

    async def _generate_internal(self, params: Dict[str, Any]) -> AsyncGenerator[Request, None]:
        """
        Internal generator that calls the module instance and enriches requests.
        """
        async for req in self.module.generate(self.config, params, self.login_info):
            # Enrich request with module context
            # Force context from module wrapper to ensure consistency
            req.account = self.account.name
            req.platform = self.platform.name
            req.module = self.name
            req.run_id = self.run_id

            # Attach configured middlewares
            if self.download_middleware:
                req.download_middleware.extend(self.download_middleware)
            if self.data_middleware:
                req.data_middleware.extend(self.data_middleware)
            
            # Attach context if pending
            if self.pending_ctx:
                req.context = self.pending_ctx
                
            yield req

    async def dispatch(self, response: Response) -> ParserData:
        """
        Dispatches response to the module parser.
        """
        # Call parser
        result = await self.module.parser(response, self.config)
        
        # We could enrich the result here if needed
        # e.g. set run_id on error_task if missing
        if result.error_task and not result.error_task.run_id:
             result.error_task.run_id = response.run_id
             
        return result
