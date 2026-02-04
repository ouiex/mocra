from enum import Enum
from typing import Dict, Optional
from pydantic import BaseModel, Field

class ErrorCategory(str, Enum):
    DOWNLOAD = "download"
    PARSE = "parse"
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    OTHER = "other"

class ErrorSeverity(str, Enum):
    MINOR = "minor"
    MAJOR = "major"
    FATAL = "fatal"

class ErrorStats(BaseModel):
    total_errors: int = 0
    success_count: int = 0
    consecutive_errors: int = 0
    errors_by_category: Dict[ErrorCategory, int] = Field(default_factory=dict)
    last_error_time: Optional[int] = None
    last_success_time: Optional[int] = None
    is_task_terminated: bool = False
    is_module_terminated: bool = False

    def error_rate(self) -> float:
        total = self.total_errors + self.success_count
        if total == 0:
            return 0.0
        return self.total_errors / total

    def health_score(self) -> float:
        error_rate = self.error_rate()
        consecutive_penalty = min(self.consecutive_errors * 0.1, 0.5)
        return max(0.0, 1.0 - error_rate - consecutive_penalty)
