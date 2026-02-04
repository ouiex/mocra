import logging
import time
from typing import Optional, Tuple
from common.models.message import Request, Response
from common.interfaces.processor import BaseProcessor, ProcessorContext, ProcessorResult, ProcessorResultType, RetryPolicy
from common.state import get_state
from engine.core.events import (
    EventDownloadStarted,
    EventDownloadCompleted,
    EventDownloadFailed,
    EventResponsePublish,
    ResponseEventPayload,
)
from common.status_tracker import ErrorDecision

logger = logging.getLogger(__name__)

# -- 1. ConfigProcessor --
class ConfigProcessor(BaseProcessor[Request, Request]):
    @property
    def name(self) -> str:
        return "ConfigProcessor"

    async def process(self, input_item: Request, context: ProcessorContext) -> ProcessorResult[Request]:
        # Rust: merges ModuleConfig. Python: We assume Request.meta or similar carries config.
        # Logic: Ensure necessary config is loaded (maybe from ModuleManager?)
        # For now, pass-through.
        return ProcessorResult.success(input_item)

# -- 2. ProxyMiddlewareProcessor --
class ProxyProcessor(BaseProcessor[Request, Request]):
    @property
    def name(self) -> str:
        return "ProxyProcessor"

    async def process(self, input_item: Request, context: ProcessorContext) -> ProcessorResult[Request]:
        state = get_state()
        if not state.proxy_manager:
            return ProcessorResult.success(input_item)
            
        # Check if proxy enabled (Rust: check config)
        if not input_item.meta.get("enable_proxy", True): # Default to true?
             # Or Rust logic: config.get("enable_proxy")
             return ProcessorResult.success(input_item)
             
        # Rust: proxy_manager.get_proxy(None)
        # Python: we used state.proxy_manager.get_proxy()
        proxy = await state.proxy_manager.get_proxy()
        if proxy:
            # Rust sets req.proxy. Python Request doesn't have it, so we use meta as agreed convention.
            input_item.meta["proxy"] = proxy.url
            logger.debug(f"[ProxyProcessor] Assigned proxy {proxy.url} to {input_item.id}")
        else:
            # If fail, should we retry? Rust implementation retries if error getting proxy.
            # Assuming get_proxy returns None on soft fail.
            pass
            
        return ProcessorResult.success(input_item)

# -- 3. RequestMiddlewareProcessor --
class RequestMiddlewareProcessor(BaseProcessor[Request, Request]):
    @property
    def name(self) -> str:
        return "RequestMiddlewareProcessor"
        
    async def process(self, input_item: Request, context: ProcessorContext) -> ProcessorResult[Request]:
        state = get_state()
        if not state.middleware_manager:
              return ProcessorResult.success(input_item)
              
        # Rust: middleware_manager.handle_request(req, config)
        # Python: process_request usually returns Request or None
        
        try:
            processed_req = await state.middleware_manager.process_request(input_item)
            if processed_req is None:
                # If dropped
                logger.info(f"[RequestMiddleware] Dropped request {input_item.id}")
                # We need to return a result that stops the chain but isn't necessarily a fatal error.
                # However, ProcessorResult expected OutputT=Request.
                # If we return success(None), next processor fails.
                # We could signal a specific "Drop" state, or return Filtered.
                # For now, let's throw a special error or handle gracefully.
                return ProcessorResult.fatal(Exception("Request dropped by middleware"))
                
            return ProcessorResult.success(processed_req)
        except Exception as e:
            logger.error(f"[RequestMiddleware] Error: {e}")
            return ProcessorResult.retry(e)

# -- 4. DownloadProcessor --
class DownloadProcessor(BaseProcessor[Request, Optional[Response]]):
    @property
    def name(self) -> str:
        return "DownloadProcessor"

    async def process(self, input_item: Request, context: ProcessorContext) -> ProcessorResult[Optional[Response]]:
        state = get_state()
        status = state.status_tracker
        
        # 0. Retry check? Rust checks is_retry.
        is_retry = context.retry_policy.current_retry > 0
        
        if not is_retry and status:
            # 1. Task Check
            decision = await status.should_task_continue(input_item.account) # task_id?
            if decision == ErrorDecision.TERMINATE:
                 return ProcessorResult.fatal(Exception("Task terminated"))
            
            # 2. Module Check
            mod_id = input_item.module_id
            decision = await status.should_module_continue(mod_id)
            if decision == ErrorDecision.TERMINATE:
                 await status.release_module_locker(mod_id)
                 return ProcessorResult.success(None) # Skip
        
        # 3. Download
        if not state.downloader:
             return ProcessorResult.fatal(Exception("No Downloader"))

        try:
            if state.event_bus:
                state.event_bus.publish(EventDownloadStarted(url=input_item.url))
            start = time.time()
            response = await state.downloader.download(input_item)
            duration = time.time() - start
            if state.event_bus:
                state.event_bus.publish(
                    EventDownloadCompleted(
                        url=input_item.url,
                        status_code=response.status_code,
                        duration_ms=duration * 1000,
                    )
                )
            
            # Enrich
            response.metadata.add_trait_config('latency', duration)
            response.metadata.add_trait_config('request_id', str(input_item.id))
            response.metadata.add_trait_config('url', input_item.url) # Save URL in meta for logging
            
            # 4. Success Record
            if status:
                await status.record_download_success(str(input_item.id))
            
            return ProcessorResult.success(response)

        except Exception as e:
            # 5. Connect Error / Timeout Handling
            logger.warning(f"[DownloadProcessor] Error: {e}")
            if state.event_bus:
                state.event_bus.publish(EventDownloadFailed(url=input_item.url, error=str(e)))
            
            # Retry policy
            if context.retry_policy.max_retries > context.retry_policy.current_retry:
                 return ProcessorResult.retry(e, context)
            
            # Record Error
            if status:
                decision = await status.record_download_error(
                    input_item.account, input_item.module_id, str(input_item.id), e
                )
                if decision == ErrorDecision.TERMINATE:
                     return ProcessorResult.fatal(e)
            
            # Fail gracefully?
            return ProcessorResult.success(None)

# -- 5. ResponseMiddlewareProcessor --
class ResponseMiddlewareProcessor(BaseProcessor[Optional[Response], Optional[Response]]):
    @property
    def name(self) -> str:
        return "ResponseMiddlewareProcessor"

    async def process(self, input_item: Optional[Response], context: ProcessorContext) -> ProcessorResult[Optional[Response]]:
        if input_item is None:
            return ProcessorResult.success(None)
            
        state = get_state()
        if not state.middleware_manager:
            return ProcessorResult.success(input_item)
            
        # Rust: middleware_manager.handle_response
        try:
            processed_resp = await state.middleware_manager.process_response(input_item)
            return ProcessorResult.success(processed_resp)
        except Exception as e:
             return ProcessorResult.retry(e)

# -- 6. ResponseOffloadProcessor --
class ResponseOffloadProcessor(BaseProcessor[Optional[Response], Optional[Response]]):
    @property
    def name(self) -> str:
        return "ResponseOffloadProcessor"

    async def process(self, input_item: Optional[Response], context: ProcessorContext) -> ProcessorResult[Optional[Response]]:
        if input_item is None:
            return ProcessorResult.success(None)

        state = get_state()
        if not state.blob_storage:
            return ProcessorResult.success(input_item)

        try:
            import os

            threshold = int(os.environ.get("BLOB_OFFLOAD_THRESHOLD", "1048576"))
            if input_item.should_offload(threshold):
                await input_item.offload(state.blob_storage)
            return ProcessorResult.success(input_item)
        except Exception as e:
            logger.error(f"[Offload] Failed to offload response: {e}")
            return ProcessorResult.success(input_item)

# -- 6. ResponsePublishProcessor --
# -- 7. ResponsePublishProcessor --
class ResponsePublishProcessor(BaseProcessor[Optional[Response], None]):
    @property
    def name(self) -> str:
        return "ResponsePublishProcessor"

    async def process(self, input_item: Optional[Response], context: ProcessorContext) -> ProcessorResult[None]:
        if input_item is None:
            return ProcessorResult.success(None)
            
        state = get_state()
        
        # Rust: queue_manager.try_send_local_response -> fallback
        # Python: state.mq.publish_response
        if state.mq:
            try:
                await state.mq.publish_response(input_item)
                if state.event_bus:
                    state.event_bus.publish(
                        EventResponsePublish(
                            payload=ResponseEventPayload(
                                account=input_item.account,
                                platform=input_item.platform,
                                module=input_item.module,
                                request_id=str(input_item.id),
                                status_code=input_item.status_code,
                            )
                        )
                    )
            except Exception as e:
                return ProcessorResult.retry(e)
                
        # Rust also locks module in pre_process and releases in error/post. 
        # For simplicity, we assume status tracker logic handles locks elsewhere or we add hooks.
        
        return ProcessorResult.success(None)
