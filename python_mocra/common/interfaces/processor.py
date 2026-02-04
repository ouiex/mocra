from typing import Generic, TypeVar, Protocol, runtime_checkable
from enum import Enum
from abc import ABC, abstractmethod

InputT = TypeVar("InputT", contravariant=True)
OutputT = TypeVar("OutputT", covariant=True)

class RetryPolicy:
    def __init__(self, max_retries: int = 3, current_retry: int = 0, reason: str = ""):
        self.max_retries = max_retries
        self.current_retry = current_retry
        self.reason = reason

class ProcessorContext:
    """
    Context passed down through the processor chain.
    """
    def __init__(self, retry_policy: RetryPolicy = None):
        self.retry_policy = retry_policy or RetryPolicy()

class ProcessorResultType(Enum):
    SUCCESS = "success"
    RETRYABLE_FAILURE = "retryable_failure"
    FATAL_FAILURE = "fatal_failure"

class ProcessorResult(Generic[OutputT]):
    def __init__(
        self, 
        status: ProcessorResultType, 
        data: OutputT = None, 
        error: Exception = None,
        context: ProcessorContext = None
    ):
        self.status = status
        self.data = data
        self.error = error
        self.context = context

    @classmethod
    def success(cls, data: OutputT) -> "ProcessorResult[OutputT]":
        return cls(status=ProcessorResultType.SUCCESS, data=data)

    @classmethod
    def retry(cls, error: Exception, context: ProcessorContext = None) -> "ProcessorResult[OutputT]":
        return cls(status=ProcessorResultType.RETRYABLE_FAILURE, error=error, context=context)

    @classmethod
    def fatal(cls, error: Exception) -> "ProcessorResult[OutputT]":
        return cls(status=ProcessorResultType.FATAL_FAILURE, error=error)

class BaseProcessor(ABC, Generic[InputT, OutputT]):
    """
    Abstract base class for all Processors (TaskProcessor, DownloadProcessor, etc.)
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    async def process(self, input_item: InputT, context: ProcessorContext) -> ProcessorResult[OutputT]:
        pass
