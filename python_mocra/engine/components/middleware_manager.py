from typing import List, Optional
import logging
from common.interfaces.middleware import BaseDownloadMiddleware, BaseDataMiddleware
from common.models.message import Request, Response
from common.models.data import Data

logger = logging.getLogger(__name__)

class MiddlewareManager:
    def __init__(self):
        self.download_middlewares: List[BaseDownloadMiddleware] = []
        self.data_middlewares: List[BaseDataMiddleware] = []

    def register_download_middleware(self, middleware: BaseDownloadMiddleware):
        self.download_middlewares.append(middleware)
        logger.info(f"Registered download middleware: {middleware.name}")

    def register_data_middleware(self, middleware: BaseDataMiddleware):
        self.data_middlewares.append(middleware)
        logger.info(f"Registered data middleware: {middleware.name}")

    async def process_request(self, request: Request) -> Optional[Request]:
        for mw in self.download_middlewares:
            try:
                result = await mw.before_request(request)
                if result is None:
                    logger.debug(f"Middleware {mw.name} dropped the request")
                    return None
                request = result
            except Exception as e:
                logger.error(f"Middleware {mw.name} failed in before_request: {e}")
                # We optionally continue or raise. Current policy: continue but log.
        return request

    async def process_response(self, response: Response) -> Response:
        # Process in reverse order for response? Typically yes, symmetric onion model.
        # But here valid list order is also fine. Let's do reverse to match onion.
        for mw in reversed(self.download_middlewares):
            try:
                response = await mw.after_response(response)
            except Exception as e:
                logger.error(f"Middleware {mw.name} failed in after_response: {e}")
        return response

    async def process_data(self, data: Data) -> Data:
        for mw in self.data_middlewares:
            try:
                data = await mw.handle_data(data)
            except Exception as e:
                logger.error(f"Middleware {mw.name} failed in handle_data: {e}")
        return data
