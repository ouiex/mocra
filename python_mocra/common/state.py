from typing import Optional, TYPE_CHECKING, Any
import redis.asyncio as redis
from contextlib import asynccontextmanager
from .config import settings, AppConfig

if TYPE_CHECKING:
    from downloader.manager import DownloaderManager
    from engine.components.module_manager import ModuleManager
    from engine.components.middleware_manager import MiddlewareManager
    from mq.interface import MqBackend
    from utils.rate_limit import BaseRateLimiter
    from common.status_tracker import StatusTracker
    from engine.task import TaskManager
    from engine.components.scheduler import CronScheduler
    from engine.components.monitor import SystemMonitor
    from engine.components.deduplication import Deduplicator
    from common.registry import NodeRegistry
    from engine.core.event_bus import EventBus
    from proxy.manager import ProxyManager
    from engine.core.metrics import MetricsExporter
    from js_v8.runtime import NodeJSRuntime
    from common.db import Database
    from cacheable.service import CacheService
    from sync.distributed import SyncService
    from utils.storage import BlobStorage

class State:
    _instance: Optional['State'] = None
    
    def __init__(self):
        self.config: AppConfig = settings
        self.redis: Optional[redis.Redis] = None
        self.db: Optional['Database'] = None
        self.mq: Optional['MqBackend'] = None
        self.status_tracker: Optional['StatusTracker'] = None
        self.downloader: Optional['DownloaderManager'] = None
        self.module_manager: Optional['ModuleManager'] = None
        self.middleware_manager: Optional['MiddlewareManager'] = None
        self.rate_limiter: Optional['BaseRateLimiter'] = None
        self.registry: Optional['NodeRegistry'] = None
        self.proxy_manager: Optional['ProxyManager'] = None
        self.task_manager: Optional['TaskManager'] = None
        self.scheduler: Optional['CronScheduler'] = None
        self.monitor: Optional['SystemMonitor'] = None
        self.deduplicator: Optional['Deduplicator'] = None
        self.event_bus: Optional['EventBus'] = None
        self.metrics_exporter: Optional['MetricsExporter'] = None
        self.js_runtime: Optional['NodeJSRuntime'] = None
        self.config_provider: Optional[Any] = None
        self.cache_service: Optional['CacheService'] = None
        self.sync_service: Optional['SyncService'] = None
        self.blob_storage: Optional['BlobStorage'] = None
        self.zombie_cleaner: Optional[Any] = None
        self._initialized = False

    def update_config(self, new_config: AppConfig):
        self.config = new_config
        # Notify other components if needed


    @classmethod
    def get(cls) -> 'State':
        if cls._instance is None:
            cls._instance = State()
        return cls._instance

    async def init(self):
        if self._initialized:
            return
            
        # Initialize Redis
        try:
            from utils.connector import create_redis_pool

            pool_size = None
            if self.config.channel_config and self.config.channel_config.redis:
                pool_size = self.config.channel_config.redis.pool_size
            if pool_size is None:
                pool_size = self.config.redis.pool_size
            self.redis = create_redis_pool(
                host=self.config.redis.host,
                port=self.config.redis.port,
                db=self.config.redis.db,
                username=None,
                password=self.config.redis.password,
                pool_size=pool_size,
                tls=False,
            )
            if self.redis is None:
                self.redis = redis.from_url(
                    self.config.redis.url,
                    encoding="utf-8",
                    decode_responses=True,
                )
            # Test connection
            await self.redis.ping()
        except Exception as e:
            import logging
            logging.warning(f"Redis connection failed: {e}. Running in standalone mode.")
            self.redis = None
        
        # Initialize Cache Service
        from cacheable.service import CacheService, RedisBackend, LocalBackend
        if self.redis:
            backend = RedisBackend(self.redis)
        else:
            backend = LocalBackend()
        
        namespace = self.config.name if hasattr(self.config, 'name') else "mocra"
        self.cache_service = CacheService(backend, namespace=namespace)
        
        # Initialize Sync Service
        from sync.distributed import SyncService
        from sync.redis_backend import RedisCoordinationBackend
        
        if self.redis:
            coord_backend = RedisCoordinationBackend(self.config.redis.url)
            self.sync_service = SyncService(coord_backend, namespace=namespace)
        else:
            # Local mode without backend
            self.sync_service = SyncService(None, namespace=namespace)
        
        # Initialize Database
        from common.db import Database
        self.db = await Database.init(self.config.database.url)

        # Initialize MQ
        if self.config.mq_backend == "kafka":
            from mq.kafka_backend import KafkaBackend
            self.mq = KafkaBackend(self.config.kafka.bootstrap_servers)
        else:
            from mq.redis_backend import RedisQueueBackend
            if self.redis:
                self.mq = RedisQueueBackend.from_config(self.config)
            else:
                # Fallback to memory backend
                from mq.memory_backend import MemoryQueueBackend
                self.mq = MemoryQueueBackend()
        
        # Initialize StatusTracker
        if self.redis:
            from common.status_tracker import StatusTracker
            self.status_tracker = StatusTracker(self.redis)
        
        self._initialized = True

    async def close(self):
        if self.sync_service:
            await self.sync_service.close()
            
        if self.mq:
            # Check if it has close method. MqBackend doesn't enforce it but RedisBackend has it.
            if hasattr(self.mq, 'close'):
                await self.mq.close()

        if self.redis:
            await self.redis.aclose()
        self._initialized = False

# Global accessor
def get_state() -> State:
    return State.get()

