from typing import Optional
from urllib.parse import urlparse
from common.interfaces.middleware import BaseDownloadMiddleware
from common.models.message import Request, Response
from common.state import get_state
from utils.distributed_rate_limit import DistributedSlidingWindowRateLimiter, RateLimitConfig
import logging

logger = logging.getLogger(__name__)

class RateLimitMiddleware(BaseDownloadMiddleware):
    @property
    def name(self) -> str:
        return "RateLimitMiddleware"

    def __init__(self, default_limit: int = 1, default_window: float = 1.0):
        self.default_limit = default_limit
        self.default_window = default_window

    async def before_request(self, request: Request) -> Request:
        state = get_state()
        if not state.rate_limiter:
            # If no limiter configured, skip
            return request

        # Determine Key
        if request.limit_id:
            key = f"limit:{request.limit_id}"
        else:
            # Fallback to domain based
            try:
                domain = urlparse(request.url).netloc
                key = f"limit:domain:{domain}"
            except:
                key = "limit:global"

        # Try to wait for token
        # TODO: read limit/window from request.meta or config if advanced
        limit = request.meta.get("rate_limit_count", self.default_limit)
        window = request.meta.get("rate_limit_window", self.default_window)

        logger.debug(f"Acquiring rate limit for {key} ({limit}/{window}s)")

        limiter = state.rate_limiter
        if isinstance(limiter, DistributedSlidingWindowRateLimiter):
            # Map limit/window to config and wait
            max_rps = limit / window if window > 0 else float(limit)
            await limiter.set_config(key, RateLimitConfig(max_requests_per_second=max_rps, window_size_millis=int(window * 1000)))
            allowed = await limiter.wait_for_token(key, timeout_seconds=30.0)
        else:
            allowed = await limiter.wait_for_token(key, limit, window, timeout=30.0)
        
        if not allowed:
            logger.warning(f"Rate Limit Timeout for {request.url}")
            # In a real system, might want to raise an exception to trigger retry logic
            # For now, we proceed but log valid warning (or arguably we should block indefinitely)
            pass
            
        return request

    async def after_response(self, response: Response) -> Response:
        return response
