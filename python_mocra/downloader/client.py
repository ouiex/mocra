import httpx
import time
import asyncio
import logging
import random
from typing import Optional, Dict, Tuple, List, Any, Protocol
from common.models.request import Request
from common.models.response import Response
from common.models.headers import Headers, HeaderItem
from common.models.cookies import Cookies, CookieItem
from utils.distributed_rate_limit import DistributedSlidingWindowRateLimiter, RateLimitConfig
from utils.redis_lock import DistributedLockManager
from cacheable.service import CacheService
from common.state import get_state
from proxy.manager import Proxy
from common.config import settings

logger = logging.getLogger(__name__)

class RateLimiterLike(Protocol):
    async def wait_for_token(self, *args, **kwargs):
        ...


class RequestDownloader:
    def __init__(
        self,
        rate_limiter: RateLimiterLike,
        lock_manager: Optional[DistributedLockManager],
        cache_service: Optional[CacheService],
        pool_size: int = 100,
        max_response_size: int = 10 * 1024 * 1024
    ):
        self.rate_limiter = rate_limiter
        self.lock_manager = lock_manager
        self.cache_service = cache_service
        self.pool_size = pool_size
        self.max_response_size = max_response_size
        
        # Flags
        self.enable_cache = False
        self.enable_locker = False
        self.enable_rate_limit = True

        # Caches (L1)
        self.headers_cache: Dict[str, Tuple[float, Optional[Headers]]] = {}
        self.cookies_cache: Dict[str, Tuple[float, Optional[Cookies]]] = {}
        
        # Proxy Clients
        self.proxy_clients: Dict[str, Tuple[httpx.AsyncClient, float]] = {}
        self.default_client = self._create_client()

    def _create_client(self, proxy: Optional[str] = None) -> httpx.AsyncClient:
        limits = httpx.Limits(max_keepalive_connections=self.pool_size, max_connections=self.pool_size + 50)
        return httpx.AsyncClient(
            http2=settings.downloader.http2,
            timeout=settings.downloader.timeout,
            limits=limits,
            proxy=proxy,
            follow_redirects=True,
            verify=False 
        )

    async def _get_client(self, proxy: Optional[str]) -> httpx.AsyncClient:
        if proxy:
            if proxy in self.proxy_clients:
                client, _ = self.proxy_clients[proxy]
                self.proxy_clients[proxy] = (client, time.time())
                return client
            
            client = self._create_client(proxy)
            if len(self.proxy_clients) < 1000:
                self.proxy_clients[proxy] = (client, time.time())
            return client
        else:
            return self.default_client

    async def _load_cached_headers(self, module_id: str, cache_headers: Optional[List[str]]) -> Optional[Headers]:
        if not cache_headers:
            return None
            
        # L1 Cache
        if module_id in self.headers_cache:
            ts, headers = self.headers_cache[module_id]
            if time.time() - ts < 5:
                if headers:
                    return Headers(headers=[h for h in headers.headers if h.key in cache_headers])
                return None

        # Redis
        if not self.cache_service:
            return None
        headers = await self.cache_service.get_headers(module_id)
        self.headers_cache[module_id] = (time.time(), headers)
        
        if headers:
            return Headers(headers=[h for h in headers.headers if h.key in cache_headers])
        return None

    async def _load_cached_cookies(self, module_id: str) -> Optional[Cookies]:
        # L1 Cache
        if module_id in self.cookies_cache:
            ts, cookies = self.cookies_cache[module_id]
            if time.time() - ts < 5:
                return cookies
        
        # Redis
        if not self.cache_service:
            return None
        cookies = await self.cache_service.get_cookies(module_id)
        self.cookies_cache[module_id] = (time.time(), cookies)
        return cookies

    async def _process_request(self, request: Request) -> Request:
        if not self.enable_cache:
            return request
        
        module_id = request.module_id
        
        # Load Headers & Cookies in parallel
        headers_task = asyncio.create_task(self._load_cached_headers(module_id, request.cache_headers))
        cookies_task = asyncio.create_task(self._load_cached_cookies(module_id))
        
        cached_headers = await headers_task
        cached_cookies = await cookies_task
        
        if cached_headers:
            request.headers.merge(cached_headers)
        if cached_cookies:
            request.cookies.merge(cached_cookies)
            
        return request

    async def _do_download(self, request: Request, pre_calculated_hash: Optional[str]) -> Response:
        if request.time_sleep_secs:
            await asyncio.sleep(request.time_sleep_secs)

        if self.enable_rate_limit:
            limit_id = request.limit_id if request.limit_id else request.module_id
            # Simplified rate limiting check
            if isinstance(self.rate_limiter, DistributedSlidingWindowRateLimiter):
                max_rps = request.meta.get("rate_limit_count", 1) / max(
                    request.meta.get("rate_limit_window", 1.0), 1e-6
                )
                window_seconds = request.meta.get("rate_limit_window", 1.0)
                await self.rate_limiter.set_config(
                    limit_id,
                    RateLimitConfig(
                        max_requests_per_second=max_rps,
                        window_size_millis=int(window_seconds * 1000),
                    ),
                )
                allowed = await self.rate_limiter.wait_for_token(limit_id, timeout_seconds=60.0)
            else:
                allowed = await self.rate_limiter.wait_for_token(limit_id, timeout=60)

            if not allowed:
                raise Exception("Rate limit exceeded timeout")

        request = await self._process_request(request)
        
        proxy_url = request.meta.get("proxy")
        if not proxy_url and request.proxy:
             # Try to use request.proxy directly if it formats to a URL
             proxy_url = str(request.proxy)
             
        # proxy_url = str(request.proxy) if request.proxy else None
        client = await self._get_client(proxy_url)
        
        start_time = time.time()
        try:
            httpx_req = client.build_request(
                method=request.method,
                url=request.url,
                headers=request.headers.to_dict(),
                cookies={c.name: c.value for c in request.cookies.cookies},
                json=request.json_data,
                data=request.form if request.form else request.body
            )
            
            resp = await client.send(httpx_req)
            await resp.aread()
            
            # Content Length Check
            if len(resp.content) > self.max_response_size:
                 raise Exception("Response too large")

        except Exception as e:
            proxy_provider = request.meta.get("proxy_provider")
            if proxy_url and proxy_provider:
                state = get_state()
                if state.proxy_manager:
                    await state.proxy_manager.report_failure(
                        Proxy(url=proxy_url, provider=proxy_provider)
                    )
            # Circuit breaker logic placeholder
            raise e

        # Extract Cookies
        response_cookies = Cookies()
        try:
            for cookie in resp.cookies.jar:
                response_cookies.add(
                    name=cookie.name,
                    value=cookie.value,
                    domain=cookie.domain if cookie.domain else ""
                )
        except Exception:
             pass

        # Handle Cache Update (Cookies)
           if self.enable_cache and self.cache_service:
               asyncio.create_task(self.cache_service.save_cookies(request.module_id, response_cookies))

        # Cache Update (Headers)
        if self.enable_cache and request.cache_headers and self.cache_service:
            headers_to_cache = Headers()
            for key in request.cache_headers:
                val = resp.headers.get(key)
                if val:
                    headers_to_cache.add(key, val)
            
            if not headers_to_cache.is_empty():
                 asyncio.create_task(self.cache_service.save_headers(request.module_id, headers_to_cache))

        response_time_ms = int((time.time() - start_time) * 1000)
        proxy_provider = request.meta.get("proxy_provider")
        if proxy_url and proxy_provider:
            state = get_state()
            if state.proxy_manager:
                await state.proxy_manager.report_success(
                    Proxy(url=proxy_url, provider=proxy_provider), response_time_ms
                )

        return Response(
            id=request.id,
            platform=request.platform,
            account=request.account,
            module=request.module,
            status_code=resp.status_code,
            headers=[(k, v) for k, v in resp.headers.items()],
            cookies=response_cookies,
            content=resp.content,
            task_retry_times=request.task_retry_times,
            metadata=request.meta,
            download_middleware=request.download_middleware,
            data_middleware=request.data_middleware,
            task_finished=request.task_finished,
            context=request.context,
            run_id=request.run_id,
            prefix_request=request.id,
            priority=request.priority,
            request_hash=pre_calculated_hash
        )

    async def download(self, request: Request) -> Response:
        request_hash = request.hash() if request.enable_cache else None
        
        if request_hash:
             cached = await self.cache_service.get_response(request_hash)
             if cached:
                 return cached

        locker_enabled = request.enable_locker if request.enable_locker is not None else self.enable_locker
        
        if locker_enabled:
            key = f"task-download-{request.module_id}-{request.run_id}"
            if await self.lock_manager.acquire_lock(key):
                try:
                    return await self._do_download(request, request_hash)
                finally:
                    await self.lock_manager.release_lock(key)
            else:
                 return await self._do_download(request, request_hash)
        else:
            return await self._do_download(request, request_hash)

    async def close(self):
        await self.default_client.aclose()
        for client, _ in self.proxy_clients.values():
            await client.aclose()
