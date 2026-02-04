import aiofiles
import os
import orjson
from typing import Optional, Dict, Any

from common.interfaces.middleware import BaseDataStoreMiddleware
from common.models.data import Data, DataType, FileStore
from common.state import get_state
from utils.batch_buffer import BatchBuffer
from utils.polars_utils import dataframe_to_ipc
from utils.type_convert import json_value_to_any_value
from utils.to_numeric import to_numeric

class JsonLinesDataStoreMiddleware(BaseDataStoreMiddleware):
    @property
    def name(self) -> str:
        return "JsonLinesDataStoreMiddleware"

    def __init__(self, file_path: str = "data_output.jsonl"):
        self.file_path = file_path
        self._initialized = False
        self._buffer: Optional[BatchBuffer[bytes]] = None
        settings = get_state().config
        self._batch_size = settings.datastore.batch_size
        self._dataframe_path = settings.datastore.dataframe_path
        self._numeric_fields = settings.datastore.numeric_fields

    async def _ensure_init(self):
        if not self._initialized:
             if self._batch_size > 1:
                 async def flush_lines(items: list[bytes]):
                     async with aiofiles.open(self.file_path, mode='ab') as f:
                         await f.write(b"".join(items))

                 self._buffer = BatchBuffer(
                     capacity=max(self._batch_size * 2, 10),
                     batch_size=self._batch_size,
                     timeout=1.0,
                     flush_callback=flush_lines,
                 )
             self._initialized = True

    async def store_data(self, data: Data) -> None:
        await self._ensure_init()
        if data.data_type == DataType.JSON:
            payload: Dict[str, Any] = data.model_dump(mode='json')
            if self._numeric_fields and isinstance(payload.get("content"), dict):
                content = payload["content"]
                for field in self._numeric_fields:
                    if field in content and isinstance(content[field], str):
                        parsed = to_numeric(content[field])
                        if parsed is not None:
                            content[field] = parsed

            line = orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE)
            if self._buffer:
                await self._buffer.add(line)
            else:
                async with aiofiles.open(self.file_path, mode='ab') as f:
                    await f.write(line)
            return

        if data.data_type == DataType.DATAFRAME:
            content = data.content
            if isinstance(content, bytes):
                ipc_bytes = content
            else:
                try:
                    import polars as pl

                    if isinstance(content, list) and content and isinstance(content[0], dict):
                        # Build DataFrame with basic type conversion
                        columns = {k: [] for k in content[0].keys()}
                        for row in content:
                            for key in columns:
                                columns[key].append(row.get(key))
                        series = {
                            key: json_value_to_any_value(values) for key, values in columns.items()
                        }
                        df = pl.DataFrame(series)
                    else:
                        df = pl.DataFrame(content)
                    ipc_bytes = dataframe_to_ipc(df)
                except Exception:
                    return

            async with aiofiles.open(self._dataframe_path, mode='ab') as f:
                await f.write(ipc_bytes)
            return

        if data.data_type == DataType.FILE:
            content = data.content
            if isinstance(content, FileStore):
                path = content.file_path or content.file_name
                async with aiofiles.open(path, mode='wb') as f:
                    await f.write(content.content)
            elif isinstance(content, bytes):
                async with aiofiles.open("file_output.bin", mode='wb') as f:
                    await f.write(content)
            return
        else:
            # Handle other types logging only for now
            pass
