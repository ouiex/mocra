import os
import time
from typing import Any, Dict, Optional, AsyncGenerator

from uuid6 import uuid7

from common.interfaces.module import BaseModule, ParserData
from common.models.message import Request, Response
from common.models.login_info import LoginInfo


class MockDevSpider(BaseModule):
    @property
    def name(self) -> str:
        return "mock.dev"

    @property
    def version(self) -> int:
        return 1

    async def generate(
        self,
        config: Any,
        params: Dict[str, Any],
        login_info: Optional[LoginInfo],
    ) -> AsyncGenerator[Request, None]:
        url = os.environ.get("MOCK_DEV_URL", "http://127.0.0.1:9009/test")
        try:
            request_count = int(os.environ.get("MOCK_DEV_COUNT", "2000"))
        except Exception:
            request_count = 2000

        for i in range(request_count):
            ts = time.time_ns()
            req_url = f"{url}?_t={ts}&_i={i}"
            yield Request(
                platform="",
                account="",
                module=self.name,
                url=req_url,
                method="GET",
                run_id=uuid7(),
            )

    async def parser(self, response: Response, config: Optional[Any] = None) -> ParserData:
        return ParserData()
