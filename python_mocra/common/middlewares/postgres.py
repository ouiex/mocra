import logging
import json
from typing import Any
from .base import DataMiddleware
from common.models.message import Data

logger = logging.getLogger(__name__)

class PostgresDataStoreMiddleware(DataMiddleware):
    def __init__(self, dsn: str, table_name: str = "crawled_data"):
        self.dsn = dsn
        self.table_name = table_name
        self.engine = None
        self.session_maker = None

    async def _init_db(self):
        try:
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
            from sqlalchemy.ext.asyncio import async_sessionmaker
            from sqlalchemy import text
            
            self.engine = create_async_engine(self.dsn, echo=False)
            self.session_maker = async_sessionmaker(self.engine, expire_on_commit=False)
            
            # Simple check or table creation could go here
            logger.info(f"Initialized Postgres connection to {self.dsn}")
        except ImportError:
            logger.error("sqlalchemy or asyncpg not installed.")

    async def handle_data(self, data: Data) -> Data:
        if not self.engine:
            await self._init_db()

        if not self.engine:
            return data

        try:
            from sqlalchemy import text
            
            # Assuming a JSONB column 'data' and metadata columns
            # This is a generic insertion. Specific schema mapping would be better.
            stmt = text(f"""
                INSERT INTO {self.table_name} (request_id, platform, account, module, data, meta, created_at)
                VALUES (:rid, :plat, :acc, :mod, :dat, :meta, NOW())
            """)
            
            payload = data.data_payload
            if hasattr(payload, "model_dump_json"):
                 json_data = payload.model_dump_json()
            elif isinstance(payload, dict) or isinstance(payload, list):
                 json_data = json.dumps(payload)
            else:
                 json_data = str(payload)

            async with self.session_maker() as session:
                await session.execute(stmt, {
                    "rid": str(data.request_id) if data.request_id else None,
                    "plat": data.platform,
                    "acc": data.account,
                    "mod": data.module,
                    "dat": json_data,
                    "meta": json.dumps(data.meta)
                })
                await session.commit()
                
            logger.debug(f"Saved data to Postgres: {data.request_id}")

        except Exception as e:
            logger.error(f"Failed to save to Postgres: {e}")
            
        return data
