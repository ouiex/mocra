import logging
import json
import sys
from typing import Any, Dict
from datetime import datetime, timezone

class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after parsing the LogRecord.
    """

    def __init__(self, node_id: str = "unknown", **kwargs):
        super().__init__(**kwargs)
        self.node_id = node_id

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "node_id": self.node_id,
        }

        if hasattr(record, "trace_id"):
            log_entry["trace_id"] = record.trace_id 
        
        if hasattr(record, "span_id"):
            log_entry["span_id"] = record.span_id 

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        if record.__dict__.get("data"):
            log_entry["data"] = record.__dict__["data"]

        return json.dumps(log_entry)

def setup_logging(
    level: str = "INFO", 
    format_type: str = "json", 
    node_id: str = "unknown"
):
    """
    Configures the root logger with the specified format.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())
    
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    
    if format_type.lower() == "json":
        formatter = JsonFormatter(node_id=node_id)
        handler.setFormatter(formatter)
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)

    root_logger.addHandler(handler)
    
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
