import logging
from common.models.message import Response, ParserTaskModel
from common.models.data import Data
from common.interfaces.processor import BaseProcessor, ProcessorContext, ProcessorResult, ProcessorResultType
from common.state import get_state
from common.interfaces.module import ParserResult as ModuleParserResult

logger = logging.getLogger(__name__)

class ResponseParserProcessor(BaseProcessor[Response, ModuleParserResult]):
    @property
    def name(self) -> str:
        return "ResponseParserProcessor"

    async def process(self, input_item: Response, context: ProcessorContext) -> ProcessorResult[ModuleParserResult]:
        state = get_state()
        if not state.module_manager:
            return ProcessorResult.fatal(Exception("ModuleManager not initialized"))

        module = state.module_manager.get_module(input_item.module)
        if not module:
            return ProcessorResult.fatal(Exception(f"Module {input_item.module} not found for parsing"))

        logger.info(f"Parsing response with module: {module.name}")
        
        try:
            # Execute parsing logic
            parse_result = await module.parser(input_item)
            return ProcessorResult.success(parse_result)
            
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            return ProcessorResult.retry(e)
