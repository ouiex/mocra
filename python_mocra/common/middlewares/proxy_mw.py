from typing import Optional
from common.interfaces.middleware import BaseDownloadMiddleware
from common.models.message import Request, Response
from common.state import get_state
from proxy.manager import Proxy
import logging

logger = logging.getLogger(__name__)

class ProxyMiddleware(BaseDownloadMiddleware):
    @property
    def name(self) -> str:
        return "ProxyMiddleware"

    def __init__(self, default_strategy: str = "default"):
        self.default_strategy = default_strategy

    async def before_request(self, request: Request) -> Request:
        # Check if proxy is already set manually in meta
        if request.meta.get("proxy"):
            return request

        # Check if this request needs a proxy (default: yes, unless disabled)
        if not request.meta.get("enable_proxy", True):
            return request

        state = get_state()
        if not state.proxy_manager:
            return request

        # Get proxy from manager
        proxy = await state.proxy_manager.get_proxy(self.default_strategy)
        if proxy:
            logger.debug(f"Assigning proxy {proxy.url} to {request.url}")
            request.meta["proxy"] = proxy.url
            request.meta["proxy_provider"] = proxy.provider
            request.meta["proxy_username"] = proxy.username
            request.meta["proxy_password"] = proxy.password
        
        return request

    async def after_response(self, response: Response) -> Response:
        # Here we could report proxy health
        proxy_url = response.meta.get("proxy")
        if proxy_url:
            state = get_state()
            if state.proxy_manager:
                proxy = Proxy(
                    url=proxy_url,
                    username=response.meta.get("proxy_username"),
                    password=response.meta.get("proxy_password"),
                    provider=response.meta.get("proxy_provider"),
                )
                if response.status_code < 400:
                    await state.proxy_manager.report_success(proxy)
                else:
                    await state.proxy_manager.report_failure(proxy)
        return response
