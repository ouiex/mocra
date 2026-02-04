from abc import ABC, abstractmethod
from typing import List, Optional, Any, AsyncIterator, Dict
from ..models.request import Request
from ..models.response import Response
from ..models.message import ParserData
from ..models.headers import Headers
from ..models.cookies import Cookies
from ..models.login_info import LoginInfo

# Placeholder for ModuleConfig until properly defined or if we treat it as generic
ModuleConfig = Any

class ModuleTrait(ABC):
    def should_login(self) -> bool:
        return True
    
    @abstractmethod
    def name(self) -> str:
        pass
    
    @abstractmethod
    def version(self) -> int:
        pass
    
    async def headers(self) -> Headers:
        return Headers()
    
    async def cookies(self) -> Cookies:
        return Cookies()
    
    async def add_step(self) -> List["ModuleNodeTrait"]:
        return []
    
    async def pre_process(self, config: Optional[ModuleConfig]) -> None:
        pass
    
    async def post_process(self, config: Optional[ModuleConfig]) -> None:
        pass
    
    def cron(self) -> Optional[Any]: # CronConfig
        return None

class ModuleNodeTrait(ABC):
    @abstractmethod
    async def generate(
        self,
        config: ModuleConfig,
        params: Dict[str, Any],
        login_info: Optional[LoginInfo]
    ) -> AsyncIterator[Request]:
        pass

    @abstractmethod
    async def parser(
        self,
        response: Response,
        config: Optional[ModuleConfig]
    ) -> ParserData:
        pass

    def retryable(self) -> bool:
        return True

BaseModule = ModuleTrait
