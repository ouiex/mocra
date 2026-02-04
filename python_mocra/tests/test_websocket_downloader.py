import os
import sys
import pytest
import asyncio

# ensure project root on path for imports
sys.path.append(os.getcwd())

from websockets.asyncio.server import serve

from downloader.websocket import WebSocketSingleShotClient
from common.models.request import Request


@pytest.mark.asyncio
async def test_websocket_single_shot_basic():
    async def handler(websocket):
        msg = await websocket.recv()
        await websocket.send(f"echo:{msg}")

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://127.0.0.1:{port}/"

        req = Request(url=url, method="GET")
        req.body = b"ping"

        client = WebSocketSingleShotClient()
        resp = await client.execute(req, timeout=2.0)

        assert resp.status_code == 101
        assert resp.content == b"echo:ping"
        assert resp.run_id == req.run_id


@pytest.mark.asyncio
async def test_websocket_recv_count():
    async def handler(websocket):
        await websocket.recv()
        await websocket.send("part1")
        await websocket.send("part2")

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://127.0.0.1:{port}/"

        req = Request(url=url, method="GET")
        req.body = b"ping"
        req.meta.add("recv_count", 2, "trait_meta")

        client = WebSocketSingleShotClient()
        resp = await client.execute(req, timeout=2.0)

        assert resp.content == b"part1\npart2"
