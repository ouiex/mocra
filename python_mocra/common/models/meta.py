from typing import Dict, Any, Optional, Union
from pydantic import BaseModel, Field
from .login_info import LoginInfo

class MetaData(BaseModel):
    task: Dict[str, Any] = Field(default_factory=dict)
    login_info: Dict[str, Any] = Field(default_factory=dict)
    module_config: Dict[str, Any] = Field(default_factory=dict)
    trait_meta: Dict[str, Any] = Field(default_factory=dict)

    def add(self, key: str, value: Any, source: str) -> "MetaData":
        if source == "task":
            self.task[key] = value
        elif source == "login_info":
            self.login_info[key] = value
        elif source == "module_config":
            self.module_config[key] = value
        elif source == "trait_meta":
            self.trait_meta[key] = value
        return self

    def add_task_config(self, task_meta: Dict[str, Any]) -> "MetaData":
        # Rust replaces the whole task value if serialized
        # Here we mimic Rust's behavior: "self.task = value"
        self.task = task_meta
        return self

    def add_login_info(self, login_info: LoginInfo) -> "MetaData":
        self.login_info = login_info.extra
        return self

    def add_module_config(self, module_config: Any) -> "MetaData":
        # Assumes module_config is a dict or model with dict
        if hasattr(module_config, "model_dump"):
            self.module_config = module_config.model_dump()
        elif isinstance(module_config, dict):
            self.module_config = module_config
        return self

    def add_trait_config(self, key: str, value: Any) -> "MetaData":
        self.trait_meta[key] = value
        return self

    def get_trait_config(self, key: str) -> Optional[Any]:
        return self.trait_meta.get(key)

    def get_login_config(self, key: str) -> Optional[Any]:
        return self.login_info.get(key)

    def get_module_config(self, key: str) -> Optional[Any]:
        return self.module_config.get(key)

    def get(self, key: str, default: Any = None) -> Any:
        # Priority: trait_meta -> module_config -> task -> login_info
        if key in self.trait_meta:
            return self.trait_meta[key]
        if key in self.module_config:
            return self.module_config[key]
        if key in self.task:
            return self.task[key]
        if key in self.login_info:
            return self.login_info[key]
        return default

    def __getitem__(self, key: str) -> Any:
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: str, value: Any):
        self.trait_meta[key] = value
