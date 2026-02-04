from typing import List, Optional, Any, Dict, Union, Tuple
from uuid import UUID
from uuid6 import uuid7
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from enum import Enum
import hashlib
import json
from .headers import Headers
from .cookies import Cookies
from .meta import MetaData
from .context import ExecutionMark
from .proxy import ProxyEnum
from .priority import Priority
from .login_info import LoginInfo

class RequestMethod(str, Enum):
    POST = "POST"
    GET = "GET"
    DELETE = "DELETE"
    OPTIONS = "OPTIONS"
    PUT = "PUT"
    HEAD = "HEAD"
    WSS = "WSS"

class Request(BaseModel):
    id: UUID = Field(default_factory=uuid7)
    platform: str = ""
    account: str = ""
    module: str = ""
    url: str
    method: str
    headers: Headers = Field(default_factory=Headers)
    cookies: Cookies = Field(default_factory=Cookies)
    retry_times: int = 0
    task_retry_times: int = 0
    use_new_client: bool = False
    timeout: int = 30
    meta: MetaData = Field(default_factory=MetaData)
    params: Optional[List[Tuple[str, str]]] = None
    json_data: Optional[Any] = Field(default=None, alias="json")
    body: Optional[bytes] = None
    form: Optional[Any] = None
    cache_headers: Optional[List[str]] = None
    proxy: Optional[ProxyEnum] = None
    limit_id: str = ""
    download_middleware: List[str] = Field(default_factory=list)
    data_middleware: List[str] = Field(default_factory=list)
    task_finished: bool = False
    time_sleep_secs: Optional[int] = None
    context: ExecutionMark = Field(default_factory=ExecutionMark)
    run_id: UUID = Field(default_factory=uuid7)
    prefix_request: UUID = Field(default_factory=lambda: UUID(int=0)) # Nil UUID
    hash_str: Optional[str] = None
    enable_cache: bool = False
    enable_locker: Optional[bool] = None
    downloader: str = "request_downloader"
    priority: Priority = Priority.NORMAL
    
    _hash_cache: Optional[str] = PrivateAttr(default=None)

    @staticmethod
    def new(url: str, method: str = "GET") -> "Request":
        return Request(url=url, method=method)

    def with_priority(self, priority: Priority) -> "Request":
        self.priority = priority
        return self

    def use_proxy(self, proxy: ProxyEnum) -> "Request":
        self.proxy = proxy
        return self
    
    @property
    def task_id(self) -> str:
        return f"{self.account}-{self.platform}"

    @property
    def module_id(self) -> str:
        return f"{self.account}-{self.platform}-{self.module}"

    def with_params(self, params: List[Tuple[str, str]]) -> "Request":
        self.params = params
        return self

    def with_headers(self, headers: Headers) -> "Request":
        self.headers.merge(headers)
        return self

    def with_cookies(self, cookies: Cookies) -> "Request":
        self.cookies.merge(cookies)
        return self
    
    def with_json(self, json_data: Any) -> "Request":
        self.json_data = json_data
        return self
    
    def with_body(self, body: bytes) -> "Request":
        self.body = body
        return self

    def with_form(self, form: Any) -> "Request":
        self.form = form
        return self
    
    def with_trait_config(self, key: str, value: Any) -> "Request":
        self.meta.add_trait_config(key, value)
        return self

    def with_login_info(self, info: LoginInfo) -> "Request":
        self.meta.add_login_info(info)
        return self
    
    def with_task_config(self, task_meta: Dict[str, Any]) -> "Request":
        self.meta.add_task_config(task_meta)
        return self
    
    def with_module_config(self, value: Any) -> "Request":
        self.meta.add_module_config(value)
        return self

    def with_sleep(self, secs: int) -> "Request":
        self.time_sleep_secs = secs
        return self
    
    def with_context(self, ctx: ExecutionMark) -> "Request":
        self.context = ctx
        return self

    def hash(self) -> str:
        if self.hash_str:
            return self.hash_str
        if self._hash_cache:
            return self._hash_cache
        
        json_str = json.dumps(self.json_data) if self.json_data is not None else "null"
        params_str = json.dumps(self.params) if self.params is not None else "null"
        form_str = json.dumps(self.form) if self.form is not None else "null"
        body_bytes = self.body if self.body else b""
        
        canonical = f"{self.account},{self.platform},{self.module},{self.url},{self.method},{params_str},{json_str},{body_bytes},{form_str}"
        
        digest = hashlib.md5(canonical.encode()).hexdigest()
        self._hash_cache = digest
        return digest

    def enable_cache_mode(self, enable: bool) -> "Request":
        self.enable_cache = enable
        return self

    def enable_cache_with(self, hash_able: Any) -> "Request":
        try:
            # Use json.dumps with sort_keys to ensure deterministic hashing for dicts
            hash_str = json.dumps(hash_able, sort_keys=True, default=str)
            self.enable_cache = True
            self.hash_str = hashlib.md5(hash_str.encode()).hexdigest()
        except (TypeError, ValueError):
            pass
        return self

    model_config = ConfigDict(arbitrary_types_allowed=True)
