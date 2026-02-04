import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from modules.mock_dev import MockDevSpider


@pytest.mark.asyncio
async def test_mock_dev_generate_count(monkeypatch):
    monkeypatch.setenv("MOCK_DEV_URL", "http://localhost:9009/test")
    monkeypatch.setenv("MOCK_DEV_COUNT", "2")

    module = MockDevSpider()
    requests = []
    async for req in module.generate({}, {}, None):
        requests.append(req)

    assert len(requests) == 2
    assert all(req.url.startswith("http://localhost:9009/test") for req in requests)
