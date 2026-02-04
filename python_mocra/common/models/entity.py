from typing import Optional, Dict, Any
from pydantic import BaseModel, ConfigDict
from datetime import datetime

class AccountModel(BaseModel):
    name: str
    password: Optional[str] = None
    properties: Dict[str, Any] = {}
    
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

class PlatformModel(BaseModel):
    name: str
    properties: Dict[str, Any] = {}
    
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)
