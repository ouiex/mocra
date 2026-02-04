"""Connection helpers for Redis and database pools (port of Rust utils::connector)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


_DB_ENGINES: Dict[str, AsyncEngine] = {}


@dataclass
class RedisPoolConfig:
    host: str
    port: int
    db: int
    username: Optional[str] = None
    password: Optional[str] = None
    pool_size: Optional[int] = None
    tls: bool = False


def create_redis_pool(
    host: str,
    port: int,
    db: int,
    username: Optional[str] = None,
    password: Optional[str] = None,
    pool_size: Optional[int] = None,
    tls: bool = False,
) -> Optional[redis.Redis]:
    try:
        pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            max_connections=pool_size or 100,
            ssl=tls,
        )
        return redis.Redis(connection_pool=pool, decode_responses=True)
    except Exception:
        return None


def db_connection(
    url: Optional[str],
    schema: Optional[str] = None,
    pool_size: Optional[int] = None,
    tls: Optional[bool] = None,
) -> Optional[AsyncEngine]:
    if not url:
        return None

    final_url = url
    if tls and "sslmode=" not in final_url and final_url.startswith("postgres"):
        joiner = "&" if "?" in final_url else "?"
        final_url = f"{final_url}{joiner}sslmode=require"

    key = f"{final_url}|{schema or ''}|{pool_size or ''}"
    if key in _DB_ENGINES:
        return _DB_ENGINES[key]

    connect_args = {}
    if schema and final_url.startswith("postgres"):
        connect_args = {"server_settings": {"search_path": schema}}

    engine = create_async_engine(
        final_url,
        pool_size=pool_size or 10,
        connect_args=connect_args,
        echo=False,
    )
    _DB_ENGINES[key] = engine
    return engine
