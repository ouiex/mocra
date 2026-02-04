from typing import Any, Dict, Optional, Literal
from pydantic import BaseModel, Field
import time

class BaseEvent(BaseModel):
    timestamp: float = Field(default_factory=time.time)
    trace_id: Optional[str] = None
    node_id: Optional[str] = None

# --- Task Events ---

class TaskEventPayload(BaseModel):
    task_id: str
    account: str
    platform: str
    run_id: str
    modules: Optional[list[str]] = None

class EventTaskStarted(BaseEvent):
    event_type: Literal["task.started"] = "task.started"
    payload: TaskEventPayload

class EventTaskCompleted(BaseEvent):
    event_type: Literal["task.completed"] = "task.completed"
    payload: TaskEventPayload
    metrics: Dict[str, Any] = {}

class EventTaskFailed(BaseEvent):
    event_type: Literal["task.failed"] = "task.failed"
    payload: TaskEventPayload
    error: str

class EventTaskReceived(BaseEvent):
    event_type: Literal["task.received"] = "task.received"
    payload: TaskEventPayload

# --- Request Events ---

class RequestEventPayload(BaseModel):
    url: str
    method: str
    request_id: Optional[str] = None
    meta: Dict[str, Any] = {}
    account: Optional[str] = None
    platform: Optional[str] = None
    module: Optional[str] = None

class EventRequestCreated(BaseEvent):
    event_type: Literal["request.created"] = "request.created"
    payload: RequestEventPayload

class EventRequestCompleted(BaseEvent):
    event_type: Literal["request.completed"] = "request.completed"
    payload: RequestEventPayload
    status_code: int
    duration_ms: float

class EventRequestFailed(BaseEvent):
    event_type: Literal["request.failed"] = "request.failed"
    payload: RequestEventPayload
    error: str

class EventRequestReceived(BaseEvent):
    event_type: Literal["request.received"] = "request.received"
    payload: RequestEventPayload

class EventRequestPublish(BaseEvent):
    event_type: Literal["request.publish"] = "request.publish"
    payload: RequestEventPayload

# --- Download Events ---
class EventDownloadStarted(BaseEvent):
    event_type: Literal["download.started"] = "download.started"
    url: str

class EventDownloadCompleted(BaseEvent):
    event_type: Literal["download.completed"] = "download.completed"
    url: str
    status_code: int
    duration_ms: float

class EventDownloadFailed(BaseEvent):
    event_type: Literal["download.failed"] = "download.failed"
    url: str
    error: str

class ResponseEventPayload(BaseModel):
    account: str
    platform: str
    module: str
    request_id: Optional[str] = None
    status_code: Optional[int] = None

class EventResponsePublish(BaseEvent):
    event_type: Literal["response.publish"] = "response.publish"
    payload: ResponseEventPayload

# Union type for all events if needed
# Event = Union[EventTaskStarted, ...]
