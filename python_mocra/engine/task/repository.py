from typing import List, Optional, Dict
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload
from common.db import Database
from common.models.db import (
    Account, Platform, Module, 
    RelAccountPlatform, RelModuleAccount, RelModulePlatform
)

class TaskRepository:
    def __init__(self, db: Database):
        self.db = db

    async def load_account(self, name: str) -> Optional[Account]:
        async with self.db.session_factory() as session:
            stmt = select(Account).where(
                Account.name == name,
                Account.enabled == True
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def load_platform(self, name: str) -> Optional[Platform]:
        async with self.db.session_factory() as session:
            stmt = select(Platform).where(
                Platform.name == name,
                Platform.enabled == True
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def load_modules_by_account_platform(self, platform_name: str, account_name: str) -> List[Module]:
        # Replicating the complex join logic from Rust
        async with self.db.session_factory() as session:
            stmt = select(Module).join(
                RelModulePlatform, Module.id == RelModulePlatform.module_id
            ).join(
                RelModuleAccount, Module.id == RelModuleAccount.module_id
            ).join(
                Platform, RelModulePlatform.platform_id == Platform.id
            ).join(
                Account, RelModuleAccount.account_id == Account.id
            ).join(
                RelAccountPlatform, and_(
                    RelModuleAccount.account_id == RelAccountPlatform.account_id,
                    RelModulePlatform.platform_id == RelAccountPlatform.platform_id
                )
            ).where(
                Module.enabled == True,
                RelModulePlatform.enabled == True,
                RelModuleAccount.enabled == True,
                RelAccountPlatform.enabled == True,
                Platform.enabled == True,
                Account.enabled == True,
                Platform.name == platform_name,
                Account.name == account_name
            )
            
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def load_module_by_account_platform_module(
        self, platform_name: str, account_name: str, module_names: List[str]
    ) -> List[Module]:
        async with self.db.session_factory() as session:
            stmt = select(Module).join(
                RelModulePlatform, Module.id == RelModulePlatform.module_id
            ).join(
                RelModuleAccount, Module.id == RelModuleAccount.module_id
            ).join(
                Platform, RelModulePlatform.platform_id == Platform.id
            ).join(
                Account, RelModuleAccount.account_id == Account.id
            ).join(
                RelAccountPlatform, and_(
                    RelModuleAccount.account_id == RelAccountPlatform.account_id,
                    RelModulePlatform.platform_id == RelAccountPlatform.platform_id
                )
            ).where(
                Module.enabled == True,
                RelModulePlatform.enabled == True,
                RelModuleAccount.enabled == True,
                RelAccountPlatform.enabled == True,
                Platform.enabled == True,
                Account.enabled == True,
                Platform.name == platform_name,
                Account.name == account_name,
                Module.name.in_(module_names)
            )
            
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def load_module_platform_relation(self, module_id: int, platform_id: int) -> Optional[RelModulePlatform]:
        async with self.db.session_factory() as session:
            stmt = select(RelModulePlatform).where(
                RelModulePlatform.module_id == module_id,
                RelModulePlatform.platform_id == platform_id,
                RelModulePlatform.enabled == True
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def load_module_account_relation(self, module_id: int, account_id: int) -> Optional[RelModuleAccount]:
        async with self.db.session_factory() as session:
            stmt = select(RelModuleAccount).where(
                RelModuleAccount.module_id == module_id,
                RelModuleAccount.account_id == account_id,
                RelModuleAccount.enabled == True
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()
