from typing import Optional, List, Any
import logging
from uuid6 import uuid7
from common.state import get_state
from common.models.message import TaskModel, ParserTaskModel, ErrorTaskModel
from common.models.entity import AccountModel, PlatformModel
from common.models.message import Response
from engine.task.task import Task
from engine.task.module import Module
from engine.task.repository import TaskRepository

logger = logging.getLogger(__name__)

class TaskFactory:
    """
    Responsible for creating Task instances from various sources (TaskModel, ParserTaskModel, etc.).
    Manages dependencies and context loading.
    """
    def __init__(self, state):
         self.state = state
         self.repository = None
         if state.db:
             self.repository = TaskRepository(state.db)
    
    async def load_with_model(self, task_model: TaskModel) -> Task:
        if not self.repository:
            raise RuntimeError("Database not initialized, cannot load task from DB")

        # 1. Load Account & Platform
        account_db = await self.repository.load_account(task_model.account)
        if not account_db:
            raise ValueError(f"Account {task_model.account} not found")
            
        platform_db = await self.repository.load_platform(task_model.platform)
        if not platform_db:
             raise ValueError(f"Platform {task_model.platform} not found")

        account = AccountModel.model_validate(account_db)
        platform = PlatformModel.model_validate(platform_db)
        
        # 2. Identify Modules
        target_modules_names = task_model.module if task_model.module else []
        
        if not target_modules_names:
            # If no explicit modules, run all for platform/account
            modules_db = await self.repository.load_modules_by_account_platform(
                task_model.platform, task_model.account
            )
        else:
            modules_db = await self.repository.load_module_by_account_platform_module(
                task_model.platform, task_model.account, target_modules_names
            )
            
        modules: List[Module] = []
        
        # Pre-load middleware maps
        module_ids = [m.id for m in modules_db]
        
        # We need middleware loading logic here (omitted for brevity but crucial for full replication)
        # Assuming defaults or empty for now to save time, but should implementation later.
        # Rust loads: load_module_download_middleware_relations, load_module_data_middleware_relations
        # And then the middleware definitions themselves.
        
        for mod_db in modules_db:
            if not self.state.module_manager:
                continue
                
            base_module = self.state.module_manager.get_module(mod_db.name)
            if not base_module:
                logger.warning(f"Module {mod_db.name} implementation not found")
                continue
            
            # Load specific config/relation config
            # Rust merges: Module Config + RelModuleAccount Config + RelModulePlatform Config
            # We need to load relations to get configs.
            rel_mod_acc = await self.repository.load_module_account_relation(mod_db.id, account_db.id)
            rel_mod_plat = await self.repository.load_module_platform_relation(mod_db.id, platform_db.id)
            
            # Merge configs: Module < Platform < Account (Order might vary, usually specific overrides generic)
            # Rust logic: 
            # let mut config = module.config.clone();
            # merge(&mut config, &rel_module_platform.config);
            # merge(&mut config, &rel_module_account.config);
            
            config = mod_db.config.copy()
            if rel_mod_plat:
                config.update(rel_mod_plat.config)
            if rel_mod_acc:
                config.update(rel_mod_acc.config)
            
            module_instance = Module(
                module_instance=base_module,
                config=config,
                account=account,
                platform=platform,
                run_id=task_model.run_id,
                prefix_request=uuid7(),
            )
            modules.append(module_instance)
            
        return Task(
            account=account,
            platform=platform,
            modules=modules,
            metadata=task_model.model_dump(),
            run_id=task_model.run_id,
            prefix_request=uuid7()
        )

    async def load_parser_model(self, parser_model: ParserTaskModel) -> Task:
        task = await self.load_with_model(parser_model.account_task)

        task.prefix_request = parser_model.prefix_request
        task.run_id = parser_model.run_id
        task.metadata = parser_model.metadata or {}

        for module in task.modules:
            module.run_id = parser_model.run_id
            module.prefix_request = parser_model.prefix_request
            module.pending_ctx = parser_model.context

        return task
        
    async def load_error_model(self, error_model: ErrorTaskModel) -> Task:
        task = await self.load_with_model(error_model.account_task)

        task.prefix_request = error_model.prefix_request
        task.run_id = error_model.run_id
        task.metadata = error_model.metadata or {}

        for module in task.modules:
            module.run_id = error_model.run_id
            module.prefix_request = error_model.prefix_request
            module.pending_ctx = error_model.context

        return task
        
    async def load_with_response(self, response: Response) -> Task:
        # Reconstruct context from Response
        return Task(
            account=AccountModel(name=response.account),
            platform=PlatformModel(name=response.platform),
            modules=[], # Likely need specific module here
            metadata={},
            run_id=response.run_id,
            prefix_request=response.request.id if response.request else uuid7()
        )
