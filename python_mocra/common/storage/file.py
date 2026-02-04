"""File-based blob storage (port of Rust common/storage/file)."""

from __future__ import annotations

import os
from dataclasses import dataclass

import aiofiles


@dataclass
class FileBlobStorage:
    base_path: str

    async def _ensure_dir(self, path: str) -> None:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    async def put(self, key: str, data: bytes) -> str:
        file_path = os.path.join(self.base_path, key)
        await self._ensure_dir(file_path)
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(data)
        return key

    async def get(self, key: str) -> bytes:
        file_path = os.path.join(self.base_path, key)
        async with aiofiles.open(file_path, "rb") as f:
            return await f.read()
