from typing import List, Optional, Any, Tuple
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict
from .cookies import Cookies
from .headers import Headers
from .meta import MetaData
from .context import ExecutionMark
from .priority import Priority, Prioritizable

class Response(BaseModel):
    id: UUID
    platform: str
    account: str
    module: str
    status_code: int
    cookies: Cookies
    content: bytes
    storage_path: Optional[str] = None
    headers: List[Tuple[str, str]]
    task_retry_times: int
    metadata: MetaData
    download_middleware: List[str]
    data_middleware: List[str]
    task_finished: bool
    context: ExecutionMark
    run_id: UUID
    prefix_request: UUID
    request_hash: Optional[str] = None
    priority: Priority = Priority.NORMAL

    @property
    def task_id(self) -> str:
        return f"{self.account}-{self.platform}"

    @property
    def module_id(self) -> str:
        return f"{self.account}-{self.platform}-{self.module}"

    def get_trait_config(self, key: str) -> Optional[Any]:
        return self.metadata.get_trait_config(key)

    def get_login_config(self, key: str) -> Optional[Any]:
        return self.metadata.get_login_config(key)

    def get_module_config(self, key: str) -> Optional[Any]:
        return self.metadata.get_module_config(key)

    def get_task_config(self, key: str) -> Optional[Any]:
        return self.metadata.get_task_config(key)

    def get_context(self) -> ExecutionMark:
        return self.context

    def with_context(self, ctx: ExecutionMark) -> "Response":
        self.context = ctx
        return self
    
    def get_priority(self) -> Priority:
        return self.priority

    def should_offload(self, threshold: int) -> bool:
        return len(self.content) > threshold and self.storage_path is None

    async def offload(self, storage) -> None:
        if not self.content:
            return
        key = f"response/{self.run_id}/{self.id}.bin"
        path = await storage.put(key, self.content)
        self.storage_path = path
        self.content = b""

    async def reload(self, storage) -> None:
        if self.storage_path and not self.content:
            self.content = await storage.get(self.storage_path)
    
    model_config = ConfigDict(arbitrary_types_allowed=True)
