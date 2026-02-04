import logging
import hashlib
from typing import Optional
from common.interfaces.middleware import BaseDownloadMiddleware
from common.models.message import Request, Response
from common.state import get_state

logger = logging.getLogger(__name__)

class DupeFilterMiddleware(BaseDownloadMiddleware):
    @property
    def name(self) -> str:
        return "DupeFilterMiddleware"

    def __init__(self):
        pass

    async def before_request(self, request: Request) -> Optional[Request]:
        if request.meta.get("dont_filter", False):
            return request

        state = get_state()
        if not state.deduplicator:
            return request

        fingerprint = self._fingerprint(request)
        
        try:
            is_new = await state.deduplicator.check_and_set(fingerprint)
            if not is_new:
                logger.info(f"Filtered duplicate request: {request.url}")
                return None # Drop
        except Exception as e:
            logger.error(f"Deduplicator check failed: {e}")
            # Fail open (allow request) if Redis fails
            
        return request

    def _fingerprint(self, request: Request) -> str:
        s = f"{request.method}:{request.url}"
        if request.body:
             s += str(request.body)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    async def after_response(self, response: Response) -> Response:
        return response
