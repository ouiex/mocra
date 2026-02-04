from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional
import asyncio
import logging
import json
from redis.asyncio import Redis
from .models import AppConfig

logger = logging.getLogger(__name__)

class ConfigProvider(ABC):
    @abstractmethod
    async def get_config(self) -> Optional[AppConfig]:
        """Fetch the current configuration."""
        pass

    @abstractmethod
    async def watch(self) -> AsyncGenerator[AppConfig, None]:
        """Yields configuration updates."""
        pass

class RedisConfigProvider(ConfigProvider):
    def __init__(self, redis_url: str, key: str = "mocra:config:global", poll_interval: int = 10):
        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self._key = key
        self._poll_interval = poll_interval
        self._running = False
        self._current_hash = None

    async def get_config(self) -> Optional[AppConfig]:
        try:
            val = await self._redis.get(self._key)
            if val:
                # Merge with existing env-based settings to partial updates work? 
                # Or assume full config replacement?
                # Rust code does `serde_json::from_str::<Config>`, implying full replacement or default fill.
                # Pydantic `model_validate_json` is strict by default but can be used.
                # Ideally, we load payload and update current settings.
                
                # For safety, let's assume the Redis config might be partial. 
                # But creating a new AppConfig from mostly empty dict will just use defaults.
                # We probably want to overlay on top of env vars.
                # But `AppConfig` is immutable-ish. 
                
                # Let's assume Redis contains the full JSON for now, or fields to override.
                data = json.loads(val)
                # We create config from env (defaults) then update with data?
                # Pydantic is tricky here. 
                # Let's just try to parse it as AppConfig.
                return AppConfig(**data)
        except Exception as e:
            logger.error(f"Failed to fetch config from Redis: {e}")
        return None

    async def watch(self) -> AsyncGenerator[AppConfig, None]:
        self._running = True
        logger.info(f"Starting Redis Config Poller on key: {self._key}")
        
        while self._running:
            try:
                new_config = await self.get_config()
                if new_config:
                    # simplistic check: dump json and compare? 
                    # Or relying on Pydantic equality if implemented
                    # Let's use json dump for comparison
                    new_hash = new_config.model_dump_json()
                    
                    if self._current_hash != new_hash:
                        logger.info("Config changed in Redis, yielding update.")
                        self._current_hash = new_hash
                        yield new_config
            except Exception as e:
                logger.error(f"Error in config watcher: {e}")
            
            await asyncio.sleep(self._poll_interval)

    async def close(self):
        self._running = False
        await self._redis.aclose()
