import os
import time
from typing import Any, Dict, Optional, AsyncGenerator

from uuid6 import uuid7

from common.interfaces.module import BaseModule, ParserData
from common.models.message import Request, Response
from common.models.data import Data
from common.models.login_info import LoginInfo


class MocDevSpider(BaseModule):
    @property
    def name(self) -> str:
        return "moc.dev"

    @property
    def version(self) -> int:
        return 1

    async def generate(
        self,
        config: Any,
        params: Dict[str, Any],
        login_info: Optional[LoginInfo],
    ) -> AsyncGenerator[Request, None]:
        url = os.environ.get("MOC_DEV_URL", "https://moc.dev")
        try:
            request_count = int(os.environ.get("MOC_DEV_COUNT", "2000"))
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
        result = ParserData()
        data = Data.from_json(
            request_id=response.prefix_request,
            platform=response.platform,
            account=response.account,
            module=self.name,
            json_data={
                "status": response.status_code,
                "length": len(response.content or b""),
                "url": response.metadata.get("url"),
            },
        )
        result.data.append(data)
        return result
