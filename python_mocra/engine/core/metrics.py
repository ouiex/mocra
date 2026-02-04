import logging
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from engine.core.event_bus import EventBus
from engine.core.events import (
    EventTaskStarted, EventTaskCompleted, EventTaskFailed,
    EventRequestCompleted, EventRequestFailed
)

logger = logging.getLogger(__name__)

class MetricsExporter:
    """
    Subscribes to EventBus and updates Prometheus metrics.
    """
    def __init__(self, event_bus: EventBus, port: int = 8000):
        self._event_bus = event_bus
        self._port = port
        self._setup_metrics()

    def _setup_metrics(self):
        # Task Metrics
        self.task_started_total = Counter(
            'mocra_tasks_started_total', 
            'Total number of tasks started',
            ['platform', 'account']
        )
        self.task_completed_total = Counter(
            'mocra_tasks_completed_total', 
            'Total number of tasks completed successfully',
            ['platform', 'account']
        )
        self.task_failed_total = Counter(
            'mocra_tasks_failed_total', 
            'Total number of tasks failed',
            ['platform', 'account', 'error_type']
        )
        self.task_active_gauge = Gauge(
            'mocra_tasks_active',
            'Number of currently active tasks'
        )

        # Request Metrics
        self.request_duration_seconds = Histogram(
            'mocra_request_duration_seconds',
            'Time spent processing requests',
            ['method', 'domain', 'status_code']
        )
        self.request_total = Counter(
            'mocra_requests_total',
            'Total HTTP requests made',
            ['method', 'domain', 'status_code']
        )

        # System Metrics
        self.worker_info = Gauge(
            'mocra_worker_info',
            'Worker node information',
            ['version', 'node_id']
        )

    async def start(self):
        try:
            # Start Prometheus HTTP Server
            # Note: start_http_server is blocking if not in thread? 
            # prometheus_client's start_http_server spins up a daemon thread by default.
            logger.info(f"Starting Metrics/Prometheus server on port {self._port}")
            start_http_server(self._port)
            
            # Subscribe to Events
            self._event_bus.subscribe("task.started", self._on_task_started)
            self._event_bus.subscribe("task.completed", self._on_task_completed)
            self._event_bus.subscribe("task.failed", self._on_task_failed)
            self._event_bus.subscribe("request.completed", self._on_request_completed)
            self._event_bus.subscribe("request.failed", self._on_request_failed)
            
        except Exception as e:
            logger.error(f"Failed to start Metrics Exporter: {e}")

    async def _on_task_started(self, event: EventTaskStarted):
        self.task_started_total.labels(
            platform=event.payload.platform,
            account=event.payload.account
        ).inc()
        self.task_active_gauge.inc()

    async def _on_task_completed(self, event: EventTaskCompleted):
        self.task_completed_total.labels(
            platform=event.payload.platform,
            account=event.payload.account
        ).inc()
        self.task_active_gauge.dec()

    async def _on_task_failed(self, event: EventTaskFailed):
        self.task_failed_total.labels(
            platform=event.payload.platform,
            account=event.payload.account,
            error_type="general" # Could extract from event.error
        ).inc()
        self.task_active_gauge.dec()

    async def _on_request_completed(self, event: EventRequestCompleted):
        # Extract domain from url usually, simplified here
        domain = "unknown" 
        if "://" in event.payload.url:
            try:
                domain = event.payload.url.split("/")[2]
            except:
                pass

        self.request_total.labels(
            method=event.payload.method,
            domain=domain,
            status_code=str(event.status_code)
        ).inc()

        self.request_duration_seconds.labels(
            method=event.payload.method,
            domain=domain,
            status_code=str(event.status_code)
        ).observe(event.duration_ms / 1000.0)

    async def _on_request_failed(self, event: EventRequestFailed):
        domain = "unknown"
        if "://" in event.payload.url:
            try:
                domain = event.payload.url.split("/")[2]
            except:
                pass
                
        self.request_total.labels(
            method=event.payload.method,
            domain=domain,
            status_code="error"
        ).inc()
