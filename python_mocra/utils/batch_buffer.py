"""Async batch buffer (port of Rust utils::batch_buffer)."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Generic, List, Optional, TypeVar

T = TypeVar("T")


class BatchBuffer(Generic[T]):
    def __init__(
        self,
        capacity: int,
        batch_size: int,
        timeout: float,
        flush_callback: Callable[[List[T]], Awaitable[None]],
    ) -> None:
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=capacity)
        self._batch_size = batch_size
        self._timeout = timeout
        self._flush_callback = flush_callback
        self._sentinel: object = object()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        buffer: List[T] = []
        while True:
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=self._timeout)
            except asyncio.TimeoutError:
                if buffer:
                    await self._flush(buffer)
                    buffer = []
                continue

            if item is self._sentinel:
                if buffer:
                    await self._flush(buffer)
                return

            buffer.append(item)  # type: ignore[arg-type]
            if len(buffer) >= self._batch_size:
                await self._flush(buffer)
                buffer = []

    async def _flush(self, items: List[T]) -> None:
        try:
            await self._flush_callback(items)
        except Exception:
            # Best-effort flush: swallow errors to avoid killing the background task
            return

    async def add(self, item: T) -> None:
        await self._queue.put(item)

    async def close(self) -> None:
        await self._queue.put(self._sentinel)
        await self._task
