class MocraError(Exception):
    """Base error for Mocra"""
    def __init__(self, message: str = None, source: Exception = None):
        self.message = message
        self.source = source
        super().__init__(self.__str__())

    def __str__(self):
        msg = self.message or self.__class__.__name__
        if self.source:
            return f"{msg}: {self.source}"
        return msg

class RequestError(MocraError):
    pass

class ResponseError(MocraError):
    pass

class CommandError(MocraError):
    pass

class ServiceError(MocraError):
    pass

class ProxyError(MocraError):
    pass

class CookieError(MocraError):
    pass

class DownloadError(MocraError):
    pass

class QueueError(MocraError):
    pass

class OrmError(MocraError):
    pass

class ModuleError(MocraError):
    pass

class HeaderError(MocraError):
    pass

class RateLimitError(MocraError):
    pass

class SyncError(MocraError):
    pass

class ProcessorChainError(MocraError):
    pass

class ParserError(MocraError):
    pass

class DataMiddlewareError(MocraError):
    pass

class DataStoreError(MocraError):
    pass

class TaskError(MocraError):
    pass

class DynamicLibraryError(MocraError):
    pass

class CacheServiceError(MocraError):
    pass

class ConfigError(MocraError):
    pass

class NetworkError(MocraError):
    pass

class DatabaseError(MocraError):
    pass
