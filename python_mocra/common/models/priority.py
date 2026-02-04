from enum import Enum, IntEnum
from typing import Protocol, runtime_checkable

class Priority(IntEnum):
    LOW = 1
    NORMAL = 5
    HIGH = 10

    @property
    def suffix(self) -> str:
        if self == Priority.LOW:
            return "low"
        elif self == Priority.NORMAL:
            return "normal"
        elif self == Priority.HIGH:
            return "high"
        return "normal"

@runtime_checkable
class Prioritizable(Protocol):
    def get_priority(self) -> Priority:
        ...
