import io
from typing import Optional, Union, List
import logging

try:
    import polars as pl
except ImportError:
    pl = None

logger = logging.getLogger(__name__)

def dataframe_to_ipc(df: 'pl.DataFrame', compression: str = 'uncompressed') -> bytes:
    """
    Serializes a Polars DataFrame to IPC (Arrow) format bytes.
    """
    if pl is None:
        raise ImportError("Polars is not installed")
    
    buffer = io.BytesIO()
    df.write_ipc(buffer, compression=None) # compression not fully supported in standard IPC write sometimes, check version
    return buffer.getvalue()

def ipc_to_dataframe(data: bytes) -> 'pl.DataFrame':
    """
    Deserializes IPC (Arrow) format bytes to a Polars DataFrame.
    """
    if pl is None:
        raise ImportError("Polars is not installed")
    
    return pl.read_ipc(data)

def create_empty_dataframe() -> 'pl.DataFrame':
    if pl is None:
        return None
    return pl.DataFrame()
