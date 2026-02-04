import gzip
from typing import Union

try:
    import zstandard as zstd
except Exception:  # pragma: no cover - optional dependency
    zstd = None


ZSTD_MAGIC = b"\x28\xB5\x2F\xFD"
GZIP_MAGIC = b"\x1f\x8b"


def compress_payload(data: bytes, threshold: int = 1024) -> bytes:
    if len(data) <= threshold:
        return data
    if zstd is not None:
        try:
            return zstd.ZstdCompressor(level=3).compress(data)
        except Exception:
            pass
    try:
        return gzip.compress(data)
    except Exception:
        return data


def decompress_payload(payload: bytes) -> bytes:
    if len(payload) >= 4 and payload[:4] == ZSTD_MAGIC and zstd is not None:
        try:
            return zstd.ZstdDecompressor().decompress(payload)
        except Exception:
            return payload
    if len(payload) >= 2 and payload[:2] == GZIP_MAGIC:
        try:
            return gzip.decompress(payload)
        except Exception:
            return payload
    return payload
