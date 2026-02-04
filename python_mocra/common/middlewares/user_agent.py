from typing import Optional
from common.interfaces.middleware import BaseDownloadMiddleware
from common.models.message import Request, Response
from common.config import settings

class UserAgentMiddleware(BaseDownloadMiddleware):
    @property
    def name(self) -> str:
        return "UserAgentMiddleware"

    def __init__(self, user_agent: Optional[str] = None):
        self.user_agent = user_agent or settings.downloader.user_agent

    async def before_request(self, request: Request) -> Request:
        if not request.headers.contains("User-Agent"):
            request.headers.add("User-Agent", self.user_agent)
        return request

    async def after_response(self, response: Response) -> Response:
        return response
