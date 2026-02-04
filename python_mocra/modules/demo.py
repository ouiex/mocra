from typing import Any, Dict, Optional, AsyncGenerator
from common.models.login_info import LoginInfo
from common.interfaces.module import BaseModule, ParserData
from common.models.message import Request, Response, TaskModel
from common.models.data import Data
from datetime import datetime
from uuid6 import uuid7

class DemoSpider(BaseModule):
    @property
    def name(self) -> str:
        return "demo_spider"

    @property
    def version(self) -> int:
        return 1

    async def generate(self, config: Any, params: Dict[str, Any], login_info: Optional[LoginInfo]) -> AsyncGenerator[Request, None]:
        """
        Generates a request to httpbin.org based on the task.
        """
        # Note: account, platform, run_id will be filled by the Module wrapper
        yield Request(
            platform="", # filled by wrapper
            account="",  # filled by wrapper
            module=self.name,
            url=f"https://httpbin.org/json?ts={datetime.now().timestamp()}",
            method="GET",
            run_id=uuid7(), # filled/overwritten by wrapper? Wrapper says "if not req.run_id".
            # The wrapper uses self.run_id which comes from the Task.
            meta={"task_created": datetime.now().isoformat()}
        )

    async def parser(self, response: Response, config: Optional[Any] = None) -> ParserData:
        """
        Parses the JSON response from httpbin.org.
        """
        result = ParserData()
        
        # Simulating data extraction
        try:
            import json
            data = json.loads(response.content)
            
            # Create a Data object
            extracted_data = Data.from_json(
                request_id=response.prefix_request,
                platform=response.platform,
                account=response.account,
                module=self.name,
                json_data=data
            )
            result.data.append(extracted_data)
            
            # Example: Generate a follow-up request (pagination simulation)
            if "slideshow" in data:
                 # Just an example logic
                 pass

        except Exception as e:
            # Handle parsing errors
            print(f"Parsing error: {e}")
            
        return result
