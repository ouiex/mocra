from typing import Any, Dict, Optional, Union, List, Literal
from uuid import UUID
from uuid6 import uuid7
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum

class DataType(str, Enum):
    DATAFRAME = "dataframe"
    FILE = "file"
    JSON = "json"

class FileStore(BaseModel):
    file_name: str
    file_path: str
    content: bytes
    meta: Dict[str, Any] = Field(default_factory=dict)

class Data(BaseModel):
    """
    Encapsulation of extracted business data.
    """
    request_id: UUID
    platform: str
    account: str
    module: str
    
    meta: Dict[str, Any] = Field(default_factory=dict)
    
    data_type: DataType
    # In a real implementation, 'data' might be a specific model or a generic container.
    # For JSON it's a dict, for File it's FileStore info, for DataFrame it might be a serialized record list or arrow bytes.
    content: Any 
    
    data_middleware: List[str] = Field(default_factory=list)

    @staticmethod
    def from_json(request_id: UUID, platform: str, account: str, module: str, json_data: Dict[str, Any]) -> 'Data':
        return Data(
            request_id=request_id,
            platform=platform,
            account=account,
            module=module,
            data_type=DataType.JSON,
            content=json_data
        )

    model_config = ConfigDict(arbitrary_types_allowed=True)
