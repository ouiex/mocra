from typing import Optional
from pydantic import BaseModel

class ExecutionMark(BaseModel):
    module_id: Optional[str] = None
    step_idx: Optional[int] = None
    epoch: Optional[int] = None
    stay_current_step: bool = False
