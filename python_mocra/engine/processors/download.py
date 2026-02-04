import logging
import time
from common.models.message import Request, Response
from common.interfaces.processor import BaseProcessor, ProcessorContext, ProcessorResult, ProcessorResultType
from common.state import get_state

logger = logging.getLogger(__name__)

class DownloadProcessor(BaseProcessor[Request, Response]):
    @property
    def name(self) -> str:
        return "DownloadProcessor"

    async def process(self, input_item: Request, context: ProcessorContext) -> ProcessorResult[Response]:
        state = get_state()
        if not state.downloader:
             return ProcessorResult.fatal(Exception("Downloader not initialized in State"))

        try:
            # Middleware 'before_request' hook
            if state.middleware_manager:
                input_item = await state.middleware_manager.process_request(input_item)
            
            start_time = time.time()
            response = await state.downloader.download(input_item)
            duration = time.time() - start_time
            
            logger.info(f"Downloaded {input_item.url} [{response.status_code}] in {duration:.3f}s")
            
            # Middleware 'after_response' hook
            if state.middleware_manager:
                response = await state.middleware_manager.process_response(response)
            
            if response.status_code >= 400:
                # Simple error handling
                return ProcessorResult.retry(Exception(f"HTTP {response.status_code}"))
                
            return ProcessorResult.success(response)
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return ProcessorResult.retry(e)
