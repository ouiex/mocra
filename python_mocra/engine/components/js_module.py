import logging
import json
from typing import List, Optional, Any, AsyncIterator, Dict
from common.interfaces.module import BaseModule, ModuleNodeTrait
from common.models.request import Request
from common.models.response import Response
from common.models.message import ParserData
from common.models.headers import Headers
from common.models.cookies import Cookies
from common.models.login_info import LoginInfo
from common.state import get_state

logger = logging.getLogger(__name__)

class JSModuleNode(ModuleNodeTrait):
    def __init__(self, js_path: str, node_index: int):
        self.js_path = js_path
        self.node_index = node_index

    async def generate(
        self,
        config: Any,
        params: Dict[str, Any],
        login_info: Optional[LoginInfo]
    ) -> AsyncIterator[Request]:
        state = get_state()
        if not state.js_runtime:
             logger.error("JS Runtime not available")
             return

        func_name = f"steps.{self.node_index}.generate"
        login_data = login_info.model_dump() if login_info else None
        
        try:
            result = await state.js_runtime.call_function(self.js_path, func_name, [config, params, login_data])
            
            if isinstance(result, list):
                for item in result:
                    yield Request(**item)
            elif result:
                yield Request(**result)
                
        except Exception as e:
            logger.error(f"JS generate failed: {e}")

    async def parser(
        self,
        response: Response,
        config: Optional[Any]
    ) -> ParserData:
        state = get_state()
        if not state.js_runtime:
             raise RuntimeError("JS Runtime not available")

        func_name = f"steps.{self.node_index}.parser"
        resp_data = response.model_dump(mode='json')
        
        try:
            result = await state.js_runtime.call_function(self.js_path, func_name, [resp_data, config])
            return ParserData(**result)
        except Exception as e:
            logger.error(f"JS parser failed: {e}")
            raise e

class JSModule(BaseModule):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._name = "Unknown"
        self._version = 0
        self._cron = None
        self._loaded = False
        
    async def load(self):
        if self._loaded: return
        state = get_state()
        if not state.js_runtime: return
        
        try:
            self._name = await state.js_runtime.call_function(self.file_path, "name", [])
            self._version = await state.js_runtime.call_function(self.file_path, "version", [])
            try:
                self._cron = await state.js_runtime.call_function(self.file_path, "cron", [])
            except: pass
            self._loaded = True
            logger.info(f"Loaded JS module: {self._name} (v{self._version})")
        except Exception as e:
            logger.error(f"Failed to load JS module {self.file_path}: {e}")

    @property
    def name(self) -> str:
        return self._name
    
    @property
    def version(self) -> int:
        return self._version

    async def headers(self) -> Headers:
        state = get_state()
        if not state.js_runtime: return Headers()
        try:
            res = await state.js_runtime.call_function(self.file_path, "headers", [])
            if res: return Headers(**res)
            return Headers()
        except:
            return Headers()

    async def cookies(self) -> Cookies:
        state = get_state()
        if not state.js_runtime: return Cookies()
        try:
            res = await state.js_runtime.call_function(self.file_path, "cookies", [])
            if res: return Cookies(**res)
            return Cookies()
        except:
            return Cookies()

    async def add_step(self) -> List[ModuleNodeTrait]:
        state = get_state()
        if not state.js_runtime: return []
        
        try:
            escaped_path = self.file_path.replace("\\", "\\\\")
            script = f"require('{escaped_path}').steps"
            
            steps_data_str = await state.js_runtime.eval(script)
            try:
                steps_data = json.loads(steps_data_str)
            except:
                steps_data = steps_data_str

            if isinstance(steps_data, list):
                nodes = []
                for i, _ in enumerate(steps_data):
                    nodes.append(JSModuleNode(self.file_path, i))
                return nodes
            return []
        except Exception as e:
            logger.error(f"Failed to get steps for {self.name}: {e}")
            return []

    def cron(self) -> Optional[Any]:
        return self._cron
