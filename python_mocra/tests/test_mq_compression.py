import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from mq.compression import compress_payload, decompress_payload


def test_compress_decompress_roundtrip():
    data = b"a" * 2048
    compressed = compress_payload(data, threshold=100)
    assert compressed != data
    restored = decompress_payload(compressed)
    assert restored == data
