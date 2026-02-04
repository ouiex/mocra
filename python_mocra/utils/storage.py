"""Blob storage abstractions (port of Rust utils::storage)."""

from __future__ import annotations

import abc
import os
from dataclasses import dataclass
from typing import Protocol

import aiofiles


class BlobStorage(abc.ABC):
    @abc.abstractmethod
    async def put(self, key: str, data: bytes) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, key: str) -> bytes:
        raise NotImplementedError


class Offloadable(Protocol):
    def should_offload(self, threshold: int) -> bool:
        ...

    async def offload(self, storage: BlobStorage) -> None:
        ...

    async def reload(self, storage: BlobStorage) -> None:
        ...


@dataclass
class FileSystemBlobStorage(BlobStorage):
    root_path: str

    async def put(self, key: str, data: bytes) -> str:
        path = os.path.join(self.root_path, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)
        return key

    async def get(self, key: str) -> bytes:
        path = os.path.join(self.root_path, key)
        async with aiofiles.open(path, "rb") as f:
            return await f.read()
