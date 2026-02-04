import asyncio
import logging
import sys
import os
import click
from uuid6 import uuid7

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from common.models.message import TaskModel
from common.state import get_state
from engine.components.module_manager import ModuleManager
from engine.components.scheduler import CronScheduler
from engine.components.monitor import SystemMonitor
from engine.components.zombie import ZombieTaskCleaner
from engine.components.deduplication import Deduplicator
from engine.task import TaskManager
from engine.components.middleware_manager import MiddlewareManager
from common.middlewares.user_agent import UserAgentMiddleware
from common.middlewares.storage import JsonLinesDataStoreMiddleware
from common.middlewares.rate_limit import RateLimitMiddleware
from common.middlewares.proxy_mw import ProxyMiddleware
# from common.middlewares.dedup import DupeFilterMiddleware
from utils.distributed_rate_limit import DistributedSlidingWindowRateLimiter, RateLimitConfig
from common.status_tracker import StatusTracker
from proxy.manager import ProxyManager, ManualProxyProvider
from downloader.manager import DownloaderManager
from common.config import settings
from utils.logger import setup_logging
from mq.memory_backend import MemoryQueueBackend
from mq.redis_backend import RedisQueueBackend

# Setup Logging
setup_logging(
    level=settings.logging.level,
    format_type=settings.logging.format,
    node_id=settings.node_id
)
logger = logging.getLogger("main")

async def init_system():
    """Initializes the global state and components."""
    state = get_state()
    
    # Initialize State (Redis, MQ)
    try:
        await state.init()
    except Exception as e:
        logger.warning(f"State init failed (Running without Redis?): {e}")

    # Init EventBus
    from engine.core.event_bus import EventBus
    state.event_bus = EventBus()
    await state.event_bus.start()

    # Init JS Runtime
    from js_v8.runtime import NodeJSRuntime
    state.js_runtime = NodeJSRuntime()

    # 1. Modules
    state.module_manager = ModuleManager()
    await state.module_manager.load_modules()

    # Init Metrics Exporter
    from engine.core.metrics import MetricsExporter
    # Try to find a free port or use config? For now hardcoded 8000 or from env
    metrics_port = 8000
    state.metrics_exporter = MetricsExporter(state.event_bus, port=metrics_port)
    await state.metrics_exporter.start()

    # Init Task Manager
    state.task_manager = TaskManager()

    # Init Scheduler
    state.scheduler = CronScheduler()
    # Note: We don't await start() here implicitly if we want control, 
    # but for system init we should probably start it.
    # However, init_system is async.
    await state.scheduler.start()

    # Init Monitor
    state.monitor = SystemMonitor(interval=60)
    await state.monitor.start()

    # Init Node Registry
    if state.cache_service:
        from common.registry import NodeRegistry
        state.registry = NodeRegistry(state.cache_service, settings.node_id)
        # Keep reference and start loop
        asyncio.create_task(state.registry.start_loop())

        # Init Config Provider
        from common.config.provider import RedisConfigProvider
        
        async def config_watcher():
            # Use separate provider instance/connection
            provider = RedisConfigProvider(settings.redis.url)
            state.config_provider = provider
            
            async for new_cfg in provider.watch():
                logger.info("Applying new configuration from Redis")
                state.update_config(new_cfg)
                # Apply Logging Level Change immediately
                logging.getLogger().setLevel(new_cfg.logging.level)
        
        asyncio.create_task(config_watcher())

    # Init Deduplicator
    if state.redis:
        state.deduplicator = Deduplicator(state.redis, ttl=3600*24)

    # Init Blob Storage
    from common.storage import FileBlobStorage
    if settings.channel_config and settings.channel_config.blob_storage:
        blob_root = settings.channel_config.blob_storage.path
        if blob_root:
            state.blob_storage = FileBlobStorage(blob_root)
    
    # Init Zombie Cleaner (Standalone var for now, usually kept in state or background tasks)
    zombie_cleaner = ZombieTaskCleaner(interval=60, zombie_threshold_sec=300)
    await zombie_cleaner.start()
    # Store reference to prevent GC/Shutdown
    state.zombie_cleaner = zombie_cleaner

    # 2. Middlewares
    state.middleware_manager = MiddlewareManager()
    state.middleware_manager.register_download_middleware(
        UserAgentMiddleware(user_agent="Mocra/Bot V1.0")
    )
    state.middleware_manager.register_download_middleware(
        ProxyMiddleware()
    )
    state.middleware_manager.register_download_middleware(
        RateLimitMiddleware(default_limit=1, default_window=2.0) # 1 req / 2s for testing
    )
    from common.middlewares.dedup import DupeFilterMiddleware
    state.middleware_manager.register_download_middleware(
        DupeFilterMiddleware()
    )
    state.middleware_manager.register_data_middleware(
        JsonLinesDataStoreMiddleware("output_data.jsonl")
    )

    # 3. Queue & Rate Limiting Backend
        if state.redis:
            state.mq = RedisQueueBackend.from_config(settings)
    else:
         logger.warning("Redis not available, falling back to MemoryQueueBackend (Standalone Mode)")
         state.mq = MemoryQueueBackend()
    
    # Initialize Rate Limiter (Distributed Sliding Window)
    lock_manager = None
    if state.redis:
        from utils.redis_lock import DistributedLockManager

        lock_manager = DistributedLockManager(state.redis)

    state.rate_limiter = DistributedSlidingWindowRateLimiter(
        state.redis,
        lock_manager,
        namespace=settings.node_id,
        default_config=RateLimitConfig(max_requests_per_second=1.0, window_size_millis=1000),
    )

    # Initialize Proxy Manager
    state.proxy_manager = ProxyManager()
    if settings.crawler.proxy_path:
        try:
            with open(settings.crawler.proxy_path, "r", encoding="utf-8") as f:
                proxies = [line.strip() for line in f if line.strip()]
            if proxies:
                state.proxy_manager.register_provider(ManualProxyProvider(proxies))
        except Exception as exc:
            logger.warning("Failed to load proxy list: %s", exc)
    await state.proxy_manager.start(refresh_interval=300)

    # if settings.redis.host:
    #    await state.init() # connect redis
    #    state.mq = RedisStreamBackend(state.redis)
    
    # 4. Downloader
    state.downloader = DownloaderManager()
    await state.downloader.start()
    
    return state

async def shutdown_system():
    state = get_state()
    if hasattr(state, 'config_provider') and state.config_provider:
        await state.config_provider.close()
    if state.event_bus:
        await state.event_bus.stop()
    if state.registry:
        await state.registry.deregister()
    if state.scheduler:
        await state.scheduler.stop()
    if state.monitor:
        await state.monitor.stop()
    if hasattr(state, 'zombie_cleaner'):
        await state.zombie_cleaner.stop()
    if state.proxy_manager:
        await state.proxy_manager.stop()
    if state.downloader:
        await state.downloader.close()
    await state.close()

@click.group()
def cli():
    pass

@cli.command()
@click.option("--input", help="Input task definition (currently just a demo flag)")
def produce_task(input):
    """Produces a demo task into the queue."""
    async def _run():
        await init_system()
        state = get_state()
        
        task = TaskModel(
            account="user_cli",
            platform="demo_platform",
            module=["demo_spider"],
            run_id=uuid7()
        )
        logger.info(f"Publishing task: {task.task_id}")
        await state.mq.publish_task(task)
        
        # In memory backend requires producer/consumer process to be same or shared memory.
        # Since this is a separate run, MemoryQueueBackend won't work across processes.
        # This CLI structure assumes Redis for real IPC. 
        # FOR DEMO: We will just warn.
    from engine.components.scheduler import CronScheduler
    
    async def _run():
        await init_system()
        
        # Start Scheduler
        # scheduler is already started in init_system
        state = get_state()
        if not state.scheduler:
             state.scheduler = CronScheduler()
             await state.scheduler.start()
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await shutdown_system()

    asyncio.run(_run())

@cli.command()
def start_worker():
    """Starts the persistent worker node."""
    from engine.worker import UnifiedWorker
    
    async def _run():
        await init_system()
        worker = UnifiedWorker(worker_id="worker-1")
        
        # Handle shutdown signals
        try:
            await worker.start()
        except KeyboardInterrupt:
            await worker.stop()
        finally:
            await shutdown_system()

    asyncio.run(_run())

@cli.command()
def run_standalone():
    """Runs a full cycle (produce + consume) in one process for testing."""
    from engine.worker import UnifiedWorker
    
    async def _run():
        state = await init_system()
        
        # 1. Produce Task
        task = TaskModel(
            account="user_standalone",
            platform="demo_platform",
            module=["demo_spider"],
            run_id=uuid7()
        )
        await state.mq.publish_task(task)
        logger.info("Initial task published.")
        
        # 2. Start Worker (Non-blocking or run for some time)
        worker = UnifiedWorker()
        
        # Run worker processing a few times to clear queue
        logger.info("Starting worker loop (running for 10s)...")
        worker_task = asyncio.create_task(worker.start())
        
        await asyncio.sleep(10)
        await worker.stop()
        await worker_task
        
        await shutdown_system()

    asyncio.run(_run())

if __name__ == "__main__":
    cli()
