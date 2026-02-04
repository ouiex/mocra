from abc import ABC, abstractmethod
from ..models.message import Request, Response
from ..models.data import Data

class BaseDownloadMiddleware(ABC):
    """
    Abstract Base Class for Download Middleware.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Middleware name used for configuration references"""
        pass

    async def before_request(self, request: Request) -> Request:
        """
        Hook executed before the request is sent.
        Useful for signing, logging, or modifying headers.
        """
        return request

    async def after_response(self, response: Response) -> Response:
        """
        Hook executed after the response is received.
        Useful for validation or decryption.
        """
        return response

class BaseDataMiddleware(ABC):
    """
    Abstract Base Class for Data Middleware.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    async def handle_data(self, data: Data) -> Data:
        """
        Hook to process extracted data.
        """
        return data

class BaseDataStoreMiddleware(BaseDataMiddleware):
    """
    Middleware specifically for persisting data.
    """
    
    @abstractmethod
    async def store_data(self, data: Data) -> None:
        pass
    
    async def handle_data(self, data: Data) -> Data:
        # Default implementation typically chains processing then stores
        await self.store_data(data)
        return data
