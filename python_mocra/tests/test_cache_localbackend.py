import os
import sys
import pytest
import asyncio

# ensure project root on path for imports
sys.path.append(os.getcwd())

from cacheable.service import LocalBackend

@pytest.mark.asyncio
async def test_localbackend_basic_operations():
    lb = LocalBackend()

    # set/get
    await lb.set("k1", b"v1", ttl=60)
    assert await lb.get("k1") == b"v1"

    # set_nx
    ok = await lb.set_nx("nx", b"a", ttl=60)
    assert ok is True
    ok2 = await lb.set_nx("nx", b"b", ttl=60)
    assert ok2 is False

    # incr
    val1 = await lb.incr("counter", 1)
    assert val1 == 1
    val2 = await lb.incr("counter", 1)
    assert val2 == 2

    # zadd / zrangebyscore
    await lb.zadd("myz", 1.0, b"a")
    await lb.zadd("myz", 2.0, b"b")
    res = await lb.zrangebyscore("myz", 0, 10)
    assert b"a" in res and b"b" in res

    # delete
    await lb.delete("k1")
    assert await lb.get("k1") is None

