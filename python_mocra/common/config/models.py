from typing import Optional, List
from pydantic_settings import BaseSettings, SettingsConfigDict

class RedisConfig(BaseSettings):
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    pool_size: Optional[int] = None
    
    @property
    def url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"
    
    model_config = SettingsConfigDict(env_prefix="REDIS_")

class ChannelRedisConfig(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_username: Optional[str] = None
    redis_password: Optional[str] = None
    pool_size: Optional[int] = None
    shards: Optional[int] = None
    tls: Optional[bool] = None
    claim_min_idle: Optional[int] = None
    claim_count: Optional[int] = None
    claim_interval: Optional[int] = None
    listener_count: Optional[int] = None

    @property
    def url(self) -> str:
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"

    model_config = SettingsConfigDict(env_prefix="CHANNEL_REDIS_")

class BlobStorageConfig(BaseSettings):
    path: Optional[str] = None

    model_config = SettingsConfigDict(env_prefix="BLOB_STORAGE_")

class ChannelConfig(BaseSettings):
    minid_time: int = 0
    capacity: int = 10000
    queue_codec: Optional[str] = None
    batch_concurrency: Optional[int] = None
    compression_threshold: Optional[int] = None
    redis: Optional[ChannelRedisConfig] = None
    blob_storage: Optional[BlobStorageConfig] = None

    model_config = SettingsConfigDict(env_prefix="CHANNEL_CONFIG_")

class KafkaConfig(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

class DatabaseConfig(BaseSettings):
    url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/mocra"
    database_schema: Optional[str] = None
    pool_size: Optional[int] = None
    tls: Optional[bool] = None
    
    model_config = SettingsConfigDict(env_prefix="DB_")

class LoggingConfig(BaseSettings):
    level: str = "INFO"
    format: str = "json"

    model_config = SettingsConfigDict(env_prefix="LOG_")

class DownloaderConfig(BaseSettings):
    user_agent: str = "Mocra/0.1.0"
    timeout: float = 30.0
    http2: bool = True
    max_keepalive_connections: int = 100
    
    model_config = SettingsConfigDict(env_prefix="DOWNLOADER_")

class CrawlerConfig(BaseSettings):
    request_max_retries: Optional[int] = None
    task_max_errors: Optional[int] = None
    module_max_errors: Optional[int] = None
    module_locker_ttl: Optional[int] = None
    proxy_path: Optional[str] = None
    task_concurrency: Optional[int] = None
    publish_concurrency: Optional[int] = None
    dedup_ttl_secs: Optional[int] = None

    model_config = SettingsConfigDict(env_prefix="CRAWLER_")

class DataStoreConfig(BaseSettings):
    batch_size: int = 1
    numeric_fields: List[str] = []
    dataframe_path: str = "dataframe_output.ipc"
class CacheConfig(BaseSettings):
    ttl: int = 3600
    compression_threshold: Optional[int] = None
    enable_l1: Optional[bool] = None
    l1_ttl_secs: Optional[int] = None
    l1_max_entries: Optional[int] = None

    model_config = SettingsConfigDict(env_prefix="CACHE_")

class SchedulerConfig(BaseSettings):
    misfire_tolerance_secs: Optional[int] = None
    concurrency: Optional[int] = None

    model_config = SettingsConfigDict(env_prefix="SCHEDULER_")

class ApiConfig(BaseSettings):
    port: Optional[int] = None
    api_key: Optional[str] = None
    rate_limit: Optional[float] = None

    model_config = SettingsConfigDict(env_prefix="API_")

class EventBusConfig(BaseSettings):
    capacity: Optional[int] = None
    concurrency: Optional[int] = None

    model_config = SettingsConfigDict(env_prefix="EVENT_BUS_")

    model_config = SettingsConfigDict(env_prefix="DATASTORE_")

class AppConfig(BaseSettings):
    name: str = "mocra"
    app_name: str = "Mocra Python"
    node_id: str = "node-1"
    environment: str = "development"
    
    redis: RedisConfig = RedisConfig()
    kafka: KafkaConfig = KafkaConfig()
    database: DatabaseConfig = DatabaseConfig()
    mq_backend: str = "redis"
    channel_config: ChannelConfig = ChannelConfig()
    logging: LoggingConfig = LoggingConfig()
    downloader: DownloaderConfig = DownloaderConfig()
    crawler: CrawlerConfig = CrawlerConfig()
    datastore: DataStoreConfig = DataStoreConfig()
    cache: CacheConfig = CacheConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    api: ApiConfig = ApiConfig()
    event_bus: EventBusConfig = EventBusConfig()
    
    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__")
