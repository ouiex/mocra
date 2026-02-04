import logging
import asyncio
from typing import Optional, List
from websockets.asyncio.client import connect
from common.models.message import Request, Response
from common.models.cookies import Cookies

logger = logging.getLogger(__name__)

class WebSocketSingleShotClient:
    """
    Handles WebSocket requests in a single-shot manner:
    Connect -> Send (Optional) -> Recv (Timeout) -> Close.
    Matches the Request -> Response pattern.
    """
    def __init__(self):
        pass

    async def execute(self, request: Request, timeout: float = 10.0) -> Response:
        """
        Executes a WS request.
        Treats `request.body` or `request.json_payload` as the message to send.
        Waits for the first text message response.
        """
        url = request.url
        # Ensure scheme
        if url.startswith("http"):
            url = url.replace("http", "ws")
            
        try:
            # We use a short timeout for the whole operation
            headers = request.headers if request.headers else {}
            # websockets library might need list of (key, val) or simple dict
            
            async with connect(
                url, 
                additional_headers=headers,
                open_timeout=timeout
            ) as websocket:
                
                # Send message if provided
                payload = None
                if request.json_data:
                    import json
                    payload = json.dumps(request.json_data)
                elif request.body:
                    payload = request.body.decode('utf-8') if isinstance(request.body, bytes) else request.body
                
                if payload:
                    await websocket.send(payload)
                    logger.debug(f"WS Sent to {url}: {payload[:50]}...")
                
                # Receive response
                recv_count = request.meta.get("recv_count") if hasattr(request.meta, "get") else None
                recv_until = request.meta.get("recv_until") if hasattr(request.meta, "get") else None

                messages: List[bytes] = []
                try:
                    if recv_count:
                        for _ in range(int(recv_count)):
                            msg = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                            messages.append(msg.encode("utf-8") if isinstance(msg, str) else msg)
                    elif recv_until:
                        until_bytes = recv_until.encode("utf-8") if isinstance(recv_until, str) else recv_until
                        while True:
                            msg = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                            msg_bytes = msg.encode("utf-8") if isinstance(msg, str) else msg
                            messages.append(msg_bytes)
                            if until_bytes in msg_bytes:
                                break
                    else:
                        msg = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                        messages.append(msg.encode("utf-8") if isinstance(msg, str) else msg)

                    content = b"\n".join(messages)
                    return Response(
                        id=request.id,
                        platform=request.platform,
                        account=request.account,
                        module=request.module,
                        status_code=101,
                        headers=[],
                        cookies=Cookies(),
                        content=content,
                        task_retry_times=request.task_retry_times,
                        metadata=request.meta,
                        download_middleware=request.download_middleware,
                        data_middleware=request.data_middleware,
                        task_finished=request.task_finished,
                        context=request.context,
                        run_id=request.run_id,
                        prefix_request=request.prefix_request or request.id,
                        priority=request.priority,
                        request_hash=request.hash(),
                    )
                except asyncio.TimeoutError:
                    return Response(
                        id=request.id,
                        platform=request.platform,
                        account=request.account,
                        module=request.module,
                        status_code=504,
                        headers=[],
                        cookies=Cookies(),
                        content=b"WS Receive Timeout",
                        task_retry_times=request.task_retry_times,
                        metadata=request.meta,
                        download_middleware=request.download_middleware,
                        data_middleware=request.data_middleware,
                        task_finished=request.task_finished,
                        context=request.context,
                        run_id=request.run_id,
                        prefix_request=request.prefix_request or request.id,
                        priority=request.priority,
                        request_hash=request.hash(),
                    )

        except Exception as e:
            logger.error(f"WS Error {url}: {e}")
            return Response(
                id=request.id,
                platform=request.platform,
                account=request.account,
                module=request.module,
                status_code=500,
                headers=[],
                cookies=Cookies(),
                content=str(e).encode(),
                task_retry_times=request.task_retry_times,
                metadata=request.meta,
                download_middleware=request.download_middleware,
                data_middleware=request.data_middleware,
                task_finished=request.task_finished,
                context=request.context,
                run_id=request.run_id,
                prefix_request=request.prefix_request or request.id,
                priority=request.priority,
                request_hash=request.hash(),
            )
