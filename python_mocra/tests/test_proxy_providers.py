import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from proxy.providers import TextListProxyProvider


class DummyResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("http error")


class DummyClient:
    def __init__(self, text: str):
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url: str):
        return DummyResponse(self._text)


@pytest.mark.asyncio
async def test_text_list_proxy_provider(monkeypatch):
    text = "1.1.1.1:8080\n2.2.2.2:3128:user:pass\ninvalid\n"

    async def client_factory(*args, **kwargs):
        return DummyClient(text)

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: DummyClient(text))

    provider = TextListProxyProvider(name="textlist", url="http://example.com")
    proxies = await provider.get_proxies()

    assert len(proxies) == 2
    assert proxies[0].url == "http://1.1.1.1:8080"
    assert proxies[1].username == "user"
    assert proxies[1].password == "pass"
