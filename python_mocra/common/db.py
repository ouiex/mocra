from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession, AsyncEngine
from sqlalchemy.orm import DeclarativeBase
from typing import Optional
from common.config import settings
from utils.connector import db_connection

class Base(DeclarativeBase):
    pass

class Database:
    _instance: Optional['Database'] = None
    
    def __init__(self, url: str, engine: Optional[AsyncEngine] = None):
        self.engine = engine or create_async_engine(url, echo=False)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    @classmethod
    async def init(cls, url: str) -> 'Database':
        pool_size = settings.database.pool_size if hasattr(settings.database, "pool_size") else None
        engine = db_connection(url, pool_size=pool_size)
        cls._instance = cls(url, engine=engine)
        if "sqlite" in url:
            async with cls._instance.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        return cls._instance

    @classmethod
    def get(cls) -> 'Database':
        if cls._instance is None:
            raise RuntimeError("Database not initialized")
        return cls._instance

    @classmethod
    def get_session_factory(cls):
        return cls.get().session_factory
