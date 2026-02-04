import os
import sys
import pytest
import asyncio

# ensure project root on path for imports
sys.path.append(os.getcwd())

from modules.moc_dev import MocDevSpider


@pytest.mark.asyncio
async def test_moc_dev_generate_count(monkeypatch):
    monkeypatch.setenv("MOC_DEV_URL", "https://moc.dev")
    monkeypatch.setenv("MOC_DEV_COUNT", "3")

    module = MocDevSpider()
    requests = []
    async for req in module.generate({}, {}, None):
        requests.append(req)

    assert len(requests) == 3
    assert all(req.url.startswith("https://moc.dev") for req in requests)
