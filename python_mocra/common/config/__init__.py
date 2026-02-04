from .models import AppConfig, RedisConfig, LoggingConfig, DownloaderConfig
from pathlib import Path
import tomllib

# Global Config Instance (Initial load from Env)
def _deep_update(base: dict, updates: dict) -> dict:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _map_rust_config(data: dict) -> dict:
    mapped: dict = {}

    if "name" in data:
        mapped["name"] = data["name"]

    crawler = data.get("crawler", {})
    if "node_id" in crawler:
        mapped["node_id"] = crawler["node_id"]
    if "proxy_path" in crawler:
        mapped.setdefault("crawler", {})
        mapped["crawler"]["proxy_path"] = crawler["proxy_path"]
    for key in [
        "request_max_retries",
        "task_max_errors",
        "module_max_errors",
        "module_locker_ttl",
        "task_concurrency",
        "publish_concurrency",
        "dedup_ttl_secs",
    ]:
        if key in crawler:
            mapped.setdefault("crawler", {})
            mapped["crawler"][key] = crawler[key]

    db_cfg = data.get("db", {})
    if db_cfg:
        mapped.setdefault("database", {})
        if "url" in db_cfg:
            mapped["database"]["url"] = db_cfg["url"]
        if "database_schema" in db_cfg:
            mapped["database"]["database_schema"] = db_cfg["database_schema"]
        if "pool_size" in db_cfg:
            mapped["database"]["pool_size"] = db_cfg["pool_size"]
        if "tls" in db_cfg:
            mapped["database"]["tls"] = db_cfg["tls"]

    download_cfg = data.get("download_config", {})
    if download_cfg:
        mapped.setdefault("downloader", {})
        if "timeout" in download_cfg:
            mapped["downloader"]["timeout"] = download_cfg["timeout"]
        if "pool_size" in download_cfg:
            mapped["downloader"]["max_keepalive_connections"] = download_cfg["pool_size"]

    cache_cfg = data.get("cache", {})
    if cache_cfg:
        mapped.setdefault("cache", {})
        for key in [
            "ttl",
            "compression_threshold",
            "enable_l1",
            "l1_ttl_secs",
            "l1_max_entries",
        ]:
            if key in cache_cfg:
                mapped["cache"][key] = cache_cfg[key]

    scheduler_cfg = data.get("scheduler", {})
    if scheduler_cfg:
        mapped.setdefault("scheduler", {})
        for key in ["misfire_tolerance_secs", "concurrency"]:
            if key in scheduler_cfg:
                mapped["scheduler"][key] = scheduler_cfg[key]

    api_cfg = data.get("api", {})
    if api_cfg:
        mapped.setdefault("api", {})
        for key in ["port", "api_key", "rate_limit"]:
            if key in api_cfg:
                mapped["api"][key] = api_cfg[key]

    event_bus_cfg = data.get("event_bus", {})
    if event_bus_cfg:
        mapped.setdefault("event_bus", {})
        for key in ["capacity", "concurrency"]:
            if key in event_bus_cfg:
                mapped["event_bus"][key] = event_bus_cfg[key]

    logger_cfg = data.get("logger", {})
    if logger_cfg and "outputs" in logger_cfg and logger_cfg["outputs"]:
        first = logger_cfg["outputs"][0]
        if isinstance(first, dict):
            mapped.setdefault("logging", {})
            if "level" in first:
                mapped["logging"]["level"] = first["level"].upper()
            if "format" in first:
                mapped["logging"]["format"] = first["format"].lower()

    channel_cfg = data.get("channel_config", {})
    if channel_cfg:
        mapped.setdefault("channel_config", {})
        for key in ["minid_time", "capacity", "batch_concurrency", "compression_threshold", "queue_codec"]:
            if key in channel_cfg:
                mapped["channel_config"][key] = channel_cfg[key]

        if "blob_storage" in channel_cfg:
            mapped["channel_config"]["blob_storage"] = channel_cfg["blob_storage"]

        if "redis" in channel_cfg:
            mapped["channel_config"]["redis"] = channel_cfg["redis"]
            # Align primary redis config to channel redis for shared usage
            redis_cfg = channel_cfg["redis"]
            mapped["redis"] = {
                "host": redis_cfg.get("redis_host", "localhost"),
                "port": redis_cfg.get("redis_port", 6379),
                "db": redis_cfg.get("redis_db", 0),
                "username": redis_cfg.get("redis_username"),
                "password": redis_cfg.get("redis_password"),
                "pool_size": redis_cfg.get("pool_size"),
            }
            mapped["mq_backend"] = "redis"

        if "kafka" in channel_cfg:
            mapped["kafka"] = {"bootstrap_servers": channel_cfg["kafka"].get("brokers", "")}
            mapped["mq_backend"] = "kafka"

    return mapped


def _load_from_toml() -> AppConfig:
    candidates = [
        Path.cwd() / "config.toml",
        Path(__file__).resolve().parents[2] / "config.toml",
    ]
    config_path = next((p for p in candidates if p.exists()), None)
    if not config_path:
        return AppConfig()

    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    base = AppConfig().model_dump()
    mapped = _map_rust_config(raw)
    merged = _deep_update(base, mapped)
    return AppConfig.model_validate(merged)


settings = _load_from_toml()

def get_settings() -> AppConfig:
    return settings

def update_settings(new_settings: AppConfig):
    global settings
    settings = new_settings
