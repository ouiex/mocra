from enum import Enum
from typing import List, Optional, Any, Dict, Union
from uuid import UUID
from uuid6 import uuid7
from pydantic import BaseModel, Field
import time
from .context import ExecutionMark
from .priority import Priority, Prioritizable
from .response import Response
from .request import Request
from .data import Data

class TopicType(str, Enum):
    TASK = "task"
    REQUEST = "request"
    RESPONSE = "response"
    PARSER_TASK = "parser_task"
    ERROR = "error"

    @property
    def suffix(self) -> str:
        return self.value

    def get_name(self, name: str) -> str:
        return f"crawler-{name}-{self.suffix}"

class TaskModel(BaseModel):
    account: str
    platform: str
    module: Optional[List[str]] = None
    priority: Priority = Priority.NORMAL
    run_id: UUID = Field(default_factory=uuid7)

    def get_priority(self) -> Priority:
        return self.priority

    @property
    def task_id(self) -> str:
        return f"{self.account}-{self.platform}"

class ParserTaskModel(BaseModel):
    id: UUID = Field(default_factory=uuid7)
    account_task: TaskModel
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    metadata: Dict[str, Any] = Field(default_factory=dict)
    context: ExecutionMark
    run_id: UUID = Field(default_factory=uuid7)
    prefix_request: UUID

    def get_priority(self) -> Priority:
        return self.account_task.priority

    def with_context(self, ctx: ExecutionMark) -> "ParserTaskModel":
        self.context = ctx
        return self

    def stay_current_step(self) -> "ParserTaskModel":
        self.context.stay_current_step = True
        return self
    
    def get_context(self) -> ExecutionMark:
        return self.context

    def with_meta(self, meta: Dict[str, Any]) -> "ParserTaskModel":
        self.metadata = meta
        return self

    def add_meta(self, key: str, value: Any) -> "ParserTaskModel":
        self.metadata[key] = value
        return self

    def with_prefix_request(self, prefix: UUID) -> "ParserTaskModel":
        self.prefix_request = prefix
        return self
    
    @staticmethod
    def start_other_module(response: Response, module_name: str) -> "ParserTaskModel":
        return ParserTaskModel(
            account_task=TaskModel(
                account=response.account,
                platform=response.platform,
                module=[module_name],
                priority=response.priority,
                run_id=uuid7()
            ),
            context=ExecutionMark(),
            run_id=uuid7(),
            prefix_request=response.prefix_request
        )

    @staticmethod
    def from_response(response: Response) -> "ParserTaskModel":
        return ParserTaskModel(
            account_task=TaskModel(
                account=response.account,
                platform=response.platform,
                module=[response.module],
                priority=response.priority,
                run_id=response.run_id
            ),
            context=response.context,
            run_id=response.run_id,
            prefix_request=response.prefix_request,
            metadata={} 
        )

class ErrorTaskModel(BaseModel):
    id: UUID = Field(default_factory=uuid7)
    account_task: TaskModel
    error_msg: str
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    metadata: Dict[str, Any] = Field(default_factory=dict)
    context: ExecutionMark
    run_id: UUID = Field(default_factory=uuid7)
    prefix_request: UUID

    def get_priority(self) -> Priority:
        return self.account_task.priority
    
    @staticmethod
    def from_response(response: Response, error_msg: str = "") -> "ErrorTaskModel":
        meta_dict = response.metadata.model_dump() if hasattr(response.metadata, 'model_dump') else response.metadata
        return ErrorTaskModel(
            account_task=TaskModel(
                account=response.account,
                platform=response.platform,
                module=[response.module],
                priority=response.priority,
                run_id=response.run_id
            ),
            error_msg=error_msg,
            metadata=meta_dict,
            context=response.context,
            run_id=response.run_id,
            prefix_request=response.prefix_request
        )

class ParserData(BaseModel):
    data: List[Data] = Field(default_factory=list)
    parser_task: Optional[ParserTaskModel] = None
    error_task: Optional[ErrorTaskModel] = None
    stop: Optional[bool] = None

    def with_data(self, data: List[Data]) -> "ParserData":
        self.data = data
        return self
    
    def with_task(self, task: ParserTaskModel) -> "ParserData":
        self.parser_task = task
        return self

    def with_error(self, error: ErrorTaskModel) -> "ParserData":
        self.error_task = error
        return self

    def with_stop(self, stop: bool) -> "ParserData":
        self.stop = stop
        return self
