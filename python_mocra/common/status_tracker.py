from typing import Optional, Tuple, Dict
from enum import Enum
import logging
import time
import asyncio
from redis.asyncio import Redis
from sync.lock import DistributedLock
from errors import MocraError
from errors.error_stats import ErrorStats, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class ErrorDecision(Enum):
    CONTINUE = "continue"
    TERMINATE = "terminate"
    RETRY_AFTER = "retry_after"
    SKIP = "skip"

class ErrorTrackerConfig:
    def __init__(
        self,
        task_max_errors: int = 100,
        module_max_errors: int = 10,
        request_max_retries: int = 3,
        parse_max_retries: int = 3,
        enable_success_decay: bool = True,
        success_decay_amount: int = 1,
        enable_time_window: bool = False,
        time_window_seconds: int = 3600,
        consecutive_error_threshold: int = 3,
        error_ttl: int = 3600
    ):
        self.task_max_errors = task_max_errors
        self.module_max_errors = module_max_errors
        self.request_max_retries = request_max_retries
        self.parse_max_retries = parse_max_retries
        self.enable_success_decay = enable_success_decay
        self.success_decay_amount = success_decay_amount
        self.enable_time_window = enable_time_window
        self.time_window_seconds = time_window_seconds
        self.consecutive_error_threshold = consecutive_error_threshold
        self.error_ttl = error_ttl

class StatusTracker:
    def __init__(self, redis_client: Redis, config: Optional[ErrorTrackerConfig] = None):
        self.redis = redis_client
        self.config = config or ErrorTrackerConfig()
        self._locks: Dict[str, DistributedLock] = {}
        # Local cache for termination status to reduce Redis hits
        self._cache: Dict[str, Tuple[ErrorStats, float]] = {}

    def _error_key(self, task_id: str) -> str:
        return f"status:errors:{task_id}"

    async def should_task_continue(self, task_id: str) -> ErrorDecision:
        # Check cache
        if task_id in self._cache:
            stats, expiry = self._cache[task_id]
            if time.time() < expiry:
                if stats.is_task_terminated:
                    return ErrorDecision.TERMINATE
        
        # Check if task is terminated in Redis
        task_key = f"task:{task_id}:total"
        terminated_key = f"task:{task_id}:terminated"
        
        if await self.redis.exists(terminated_key):
             return ErrorDecision.TERMINATE

        # Check total errors
        errors = await self._get_error_count(task_key)
        if errors >= self.config.task_max_errors:
             await self.mark_task_terminated(task_id)
             return ErrorDecision.TERMINATE

        return ErrorDecision.CONTINUE

    async def should_module_continue(self, module_id: str) -> ErrorDecision:
        # Check cache
        if module_id in self._cache:
             stats, expiry = self._cache[module_id]
             if time.time() < expiry:
                 if stats.is_module_terminated:
                     return ErrorDecision.TERMINATE

        # Check termination flag
        terminated_key = f"module:{module_id}:terminated"
        if await self.redis.exists(terminated_key):
             return ErrorDecision.TERMINATE

        # Check errors
        download_key = f"module:{module_id}:download:total_errors"
        parse_key = f"module:{module_id}:parse:total_errors"
        
        # Using mget for efficiency
        vals = await self.redis.mget([download_key, parse_key])
        download_errs = int(vals[0]) if vals[0] else 0
        parse_errs = int(vals[1]) if vals[1] else 0
        
        total = download_errs + parse_errs
        if total >= self.config.module_max_errors:
             await self.mark_module_terminated(module_id)
             await self.release_module_locker(module_id)
             return ErrorDecision.TERMINATE
             
        return ErrorDecision.CONTINUE

    async def lock_module(self, module_id: str, ttl_ms: int = 60000) -> bool:
        """
        Locks a module (e.g., Circuit Breaker open).
        """
        lock = DistributedLock(self.redis, f"module:{module_id}", ttl_ms=ttl_ms)
        if await lock.acquire():
             self._locks[module_id] = lock
             # Also set a persistent key for inspection
             await self.redis.set(f"status:module_lock:{module_id}", "1", px=ttl_ms)
             return True
        return False

    async def release_module_locker(self, module_id: str):
        if module_id in self._locks:
            await self._locks[module_id].release()
            del self._locks[module_id]
        
        # Ensure key is gone
        await self.redis.delete(f"status:module_lock:{module_id}")
        
        # Try creating a new lock to force release if we don't hold it but want to clear it
        # (Though usually only owner should release, here it's an administrative reset)
        lock = DistributedLock(self.redis, f"module:{module_id}")
        await lock.release()

    async def record_download_success(self, request_id: str):
         request_key = f"request:{request_id}:download"
         await self._record_success(request_key)
         if request_id in self._cache:
             del self._cache[request_id]

    async def record_parse_success(self, request_id: str):
         request_key = f"request:{request_id}:parse"
         await self._record_success(request_key)
         if request_id in self._cache:
             del self._cache[request_id]

    async def record_download_error(self, task_id: str, module_id: str, request_id: str, error: Exception) -> ErrorDecision:
        category = ErrorCategory.DOWNLOAD
        severity = self._classify_severity(error)
        
        request_key = f"request:{request_id}:download"
        module_key = f"module:{module_id}:download"
        task_key = f"task:{task_id}"
        
        # Increment counters
        req_errors, mod_errors, task_errors = await asyncio.gather(
            self._increment_error(request_key, category, severity),
            self._increment_error(module_key, category, severity),
            self._increment_error(task_key, category, severity)
        )

        if severity == ErrorSeverity.FATAL:
            return ErrorDecision.TERMINATE

        if task_errors >= self.config.task_max_errors:
            return ErrorDecision.TERMINATE

        if mod_errors >= self.config.module_max_errors:
            await self.release_module_locker(module_id)
            return ErrorDecision.TERMINATE

        if req_errors >= self.config.request_max_retries:
            return ErrorDecision.SKIP

        # Retry logic
        if category == ErrorCategory.RATE_LIMIT:
             return ErrorDecision.RETRY_AFTER
             
        return ErrorDecision.RETRY_AFTER

    async def record_parse_error(self, task_id: str, module_id: str, request_id: str, error: Exception) -> ErrorDecision:
        category = ErrorCategory.PARSE
        severity = self._classify_severity(error)

        request_key = f"request:{request_id}:parse"
        module_key = f"module:{module_id}:parse"
        task_key = f"task:{task_id}"

        req_errors, mod_errors, task_errors = await asyncio.gather(
            self._increment_error(request_key, category, severity),
            self._increment_error(module_key, category, severity),
            self._increment_error(task_key, category, severity)
        )
        
        if task_errors >= self.config.task_max_errors:
             return ErrorDecision.TERMINATE
             
        if mod_errors >= self.config.module_max_errors:
             await self.release_module_locker(module_id)
             return ErrorDecision.TERMINATE
             
        if req_errors >= self.config.parse_max_retries:
             return ErrorDecision.SKIP

        return ErrorDecision.RETRY_AFTER

    async def mark_task_terminated(self, task_id: str):
        key = f"task:{task_id}:terminated"
        await self.redis.set(key, "1", ex=3600)
        # Update cache
        if task_id not in self._cache:
             self._cache[task_id] = (ErrorStats(is_task_terminated=True), time.time() + 10)
        else:
             stats, _ = self._cache[task_id]
             stats.is_task_terminated = True
             self._cache[task_id] = (stats, time.time() + 10)

    async def mark_module_terminated(self, module_id: str):
        key = f"module:{module_id}:terminated"
        await self.redis.set(key, "1", ex=3600)
        
    async def _increment_error(self, key: str, category: ErrorCategory, severity: ErrorSeverity) -> int:
        total_key = f"{key}:total_errors"
        consecutive_key = f"{key}:consecutive_errors"
        last_error_key = f"{key}:last_error_time"
        
        # Atomic increment
        total = await self.redis.incr(total_key)
        await self.redis.incr(consecutive_key)
        await self.redis.set(last_error_key, int(time.time()), ex=self.config.error_ttl)
        
        # Set expiry if new
        if total == 1:
            await self.redis.expire(total_key, self.config.error_ttl)
            await self.redis.expire(consecutive_key, self.config.error_ttl)
            await self.redis.expire(last_error_key, self.config.error_ttl)
            
        return total

    async def _record_success(self, key: str):
        total_key = f"{key}:total_errors"
        consecutive_key = f"{key}:consecutive_errors"
        success_key = f"{key}:success_count"
        last_success_key = f"{key}:last_success_time"

        now = int(time.time())
        if self.config.enable_success_decay:
            current = await self.redis.get(total_key)
            try:
                current_val = int(current) if current is not None else 0
            except Exception:
                current_val = 0
            new_val = max(0, current_val - self.config.success_decay_amount)
            await self.redis.set(total_key, new_val, ex=self.config.error_ttl)

        await self.redis.set(consecutive_key, 0, ex=self.config.error_ttl)
        await self.redis.incr(success_key)
        await self.redis.set(last_success_key, now, ex=self.config.error_ttl)

    async def _get_error_count(self, key: str) -> int:
        val = await self.redis.get(key)
        return int(val) if val else 0

    def _classify_severity(self, error: Exception) -> ErrorSeverity:
        msg = str(error).lower()
        if "auth" in msg or "unauthorized" in msg:
            return ErrorSeverity.FATAL
        if "rate limit" in msg or "too many" in msg:
            return ErrorSeverity.MAJOR
        return ErrorSeverity.MINOR
