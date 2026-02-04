from typing import Optional
from common.models.request import Request
from common.models.response import Response
from .client import RequestDownloader
from .websocket import WebSocketSingleShotClient
from common.state import get_state
from utils.distributed_rate_limit import DistributedSlidingWindowRateLimiter, RateLimitConfig
from utils.redis_lock import DistributedLockManager
from cacheable.service import CacheService
import logging

logger = logging.getLogger(__name__)

class DownloaderManager:
    """
    Manages downloader instances.
    Initializes RequestDownloader with dependencies.
    """
    def __init__(self):
        self.client: Optional[RequestDownloader] = None
        self.ws_client = WebSocketSingleShotClient()
        self.started = False

    async def start(self):
        if self.started:
            return
        
        state = get_state()
        
        # Support both Redis and local modes (reuse state limiter when possible)
        lock_manager = DistributedLockManager(state.redis) if state.redis else None
        rate_limiter = state.rate_limiter
        if not rate_limiter:
            rate_limiter = DistributedSlidingWindowRateLimiter(
                state.redis,
                lock_manager,
                namespace=state.config.node_id,
                default_config=RateLimitConfig(max_requests_per_second=1.0, window_size_millis=1000),
            )
        
        # Use cache_service from state if available
        cache_service = state.cache_service if hasattr(state, 'cache_service') and state.cache_service else None
        
        self.client = RequestDownloader(
            rate_limiter=rate_limiter,
            lock_manager=lock_manager,
            cache_service=cache_service,
        )
        self.started = True

    async def close(self):
        if self.client:
            await self.client.close()
    
    async def download(self, request: Request) -> Response:
        if not self.started or not self.client:
            await self.start()
            
        if request.url.startswith("ws") or request.method == "WSS":
             # WebSocket logic
             return await self.ws_client.execute(request)
        else:
             return await self.client.download(request)
