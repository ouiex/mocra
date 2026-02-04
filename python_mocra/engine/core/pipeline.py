from typing import Generic, List, TypeVar
import logging
import time
from common.interfaces.processor import BaseProcessor, ProcessorContext, ProcessorResult, ProcessorResultType

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")

logger = logging.getLogger("mocra.pipeline")

class Pipeline(Generic[InputT, OutputT]):
    """
    A sequential chain of processors.
    """
    def __init__(self, processors: List[BaseProcessor]):
        self.processors = processors

    async def run(self, input_item: InputT) -> ProcessorResult[OutputT]:
        current_data = input_item
        context = ProcessorContext()
        last_result = None

        # Simplified sync pipeline logic (A real pipeline might be more complex with types)
        # Note: In a statically typed language this is harder, but in Python we iterate.
        # Ideally, `processors` are compatible: P1(A)->B, P2(B)->C, ...
        
        for processor in self.processors:
            start_time = time.time()
            try:
                # logger.debug(f"Executing {processor.name}")
                result = await processor.process(current_data, context)
                
                if result.status != ProcessorResultType.SUCCESS:
                    logger.warning(f"Processor {processor.name} failed: {result.error}")
                    return result # Return failure immediately
                
                current_data = result.data
                last_result = result
                
                # Update context if needed
                if result.context:
                    context = result.context

            except Exception as e:
                logger.exception(f"Unhandled exception in processor {processor.name}")
                return ProcessorResult.fatal(e)
            
            # elapsed = time.time() - start_time
            # logger.info(f"{processor.name()} completed in {elapsed:.4f}s")
            
        return last_result
