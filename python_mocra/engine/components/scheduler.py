import asyncio
import logging
import json
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime
import time
from croniter import croniter
from sqlalchemy import select
from uuid import uuid4
from uuid6 import uuid7

from common.state import get_state
from common.models.message import TaskModel
from common.models.db import Module, Account, Platform, RelModuleAccount, RelAccountPlatform, RelModulePlatform
from common.models.priority import Priority
from common.status_tracker import StatusTracker
from sync.leader import LeaderElector

logger = logging.getLogger(__name__)

class CronScheduler:
    """
    Scheduler for triggering time-based tasks.
    Uses Leader Election to ensure unique execution.
    """
    def __init__(self, check_interval: float = 60.0):
        self.check_interval = check_interval
        self.running = False
        self._task = None
        self.state = get_state()
        self.leader_elector: Optional[LeaderElector] = None
        
        # Cache for schedules and contexts
        # Key: Module Name, Value: (CronSchedule string, List[(Account, Platform)])
        self.schedule_cache: Dict[str, Tuple[str, List[Tuple[str, str]]]] = {}
        self.cron_config_cache: Dict[str, Optional[dict]] = {}
        
        self.last_version = 0
        self.last_module_hash = 0

    async def start(self):
        if self.running:
            return
        self.running = True
        
        # Initialize Leader Elector
        redis_client = self.state.redis
        if redis_client:
            self.leader_elector = LeaderElector(redis_client, "scheduler:leader", ttl_ms=15000)
            await self.leader_elector.start()
        else:
            logger.warning("No Redis client available for Scheduler leader election. Running in standalone mode.")
            
        self._task = asyncio.create_task(self._run_loop())
        # Also start refresh loop if needed, or do it in the same loop
        asyncio.create_task(self._refresh_loop())
        
        logger.info("CronScheduler started")

    async def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        if self.leader_elector:
            await self.leader_elector.stop()
            
        logger.info("CronScheduler stopped")

    async def _refresh_loop(self):
        while self.running:
            try:
                await self.refresh_cache()
            except Exception as e:
                logger.error(f"Error in scheduler refresh loop: {e}")
            
            await asyncio.sleep(60)

    async def refresh_cache(self):
        """
        Refreshes the schedule and context cache from the database.
        """
        if not self.state.db:
            logger.warning("Database not available for scheduler refresh")
            return

        async with self.state.db.session_factory() as session:
            # Fetch all enabled contexts
            # Query equivalent to Rust's optimized single query
            stmt = (
                select(Module.name, Account.name, Platform.name, Module.cron)
                .join(RelModuleAccount, Module.id == RelModuleAccount.module_id)
                .join(Account, RelModuleAccount.account_id == Account.id)
                .join(RelAccountPlatform, Account.id == RelAccountPlatform.account_id)
                .join(Platform, RelAccountPlatform.platform_id == Platform.id)
                .join(RelModulePlatform, (Module.id == RelModulePlatform.module_id) & (Platform.id == RelModulePlatform.platform_id))
                .where(
                    RelModuleAccount.enabled == True,
                    RelAccountPlatform.enabled == True,
                    RelModulePlatform.enabled == True,
                    Module.enabled == True
                )
            )
            
            result = await session.execute(stmt)
            rows = result.all() # List of (module_name, account_name, platform_name, module_cron)

            # Rebuild cache
            new_cache = {}
            
            # Group by module
            grouped = {}
            for m_name, a_name, p_name, m_cron in rows:
                if not m_cron or not m_cron.get("enable", False):
                    continue
                
                cron_schedule = m_cron.get("schedule", "")
                if not cron_schedule:
                    continue
                    
                if m_name not in grouped:
                    grouped[m_name] = {"schedule": cron_schedule, "contexts": []}
                
                grouped[m_name]["contexts"].append((a_name, p_name))
            
            for m_name, data in grouped.items():
                new_cache[m_name] = (data["schedule"], data["contexts"])
                
            self.schedule_cache = new_cache
            logger.info(f"Scheduler cache refreshed. Active modules: {len(self.schedule_cache)}")

    async def _run_loop(self):
        last_tick = None
        
        while self.running:
            now = datetime.utcnow()
            # Truncate to minute
            current_minute = now.replace(second=0, microsecond=0)
            
            # Wait for the next minute start
            next_minute = (current_minute.timestamp() // 60 + 1) * 60
            sleep_secs = max(0, next_minute - now.timestamp())
            
            # Add a small buffer (0.1s) to ensure we are into the next minute
            await asyncio.sleep(sleep_secs + 0.1)
            
            # Re-check time
            now_check = datetime.utcnow()
            current_check_minute = now_check.replace(second=0, microsecond=0)
            
            if self.leader_elector and not self.leader_elector.is_leader:
                # Update last_tick to avoid processing old ticks if we become leader
                last_tick = current_check_minute
                continue
            
            # Leader logic
            if last_tick and current_check_minute > last_tick:
                # Process tick(s)
                # Simple logic: process current minute. 
                # Catch-up logic can be added if needed, but here we just process current.
                await self.process_tick(current_check_minute)
            elif last_tick is None:
                # First run
                await self.process_tick(current_check_minute)
                
            last_tick = current_check_minute

    async def process_tick(self, current_minute: datetime):
        tasks_to_run = []
        
        for module_name, (schedule_str, contexts) in self.schedule_cache.items():
            if self.is_schedule_match(schedule_str, current_minute):
                tasks_to_run.append((module_name, contexts))
        
        if not tasks_to_run:
            return

        logger.info(f"Scheduler tick: {len(tasks_to_run)} modules matched")
        
        start_time = time.time()
        total_triggered = 0
        
        for module_name, contexts in tasks_to_run:
            total_triggered += len(contexts)
            # Spawn processing for each module
            asyncio.create_task(self.process_module_contexts(module_name, contexts, current_minute))
            
        logger.info(f"Scheduler tick processed {total_triggered} potential tasks in {time.time() - start_time:.4f}s")

    def is_schedule_match(self, schedule_str: str, target: datetime) -> bool:
        try:
            # croniter expects local time if not specified, but we work in UTC usually. 
            # Passing current time as start time to see if it matches is tricky with croniter.
            # Standard way: check if target matches the schedule.
            # croniter.match(schedule_str, target) checks if target matches pattern
            return croniter.match(schedule_str, target)
        except Exception:
            return False

    async def process_module_contexts(self, module_name: str, contexts: List[Tuple[str, str]], current_minute: datetime):
        timestamp = int(current_minute.timestamp())
        
        # Concurrency limit could be applied here
        for account, platform in contexts:
            # Distributed Lock Check
            key = f"cron:{module_name}:{account}:{platform}:{timestamp}"
            
            # Try to acquire lock for 600s
            if self.state.redis:
                acquired = await self.state.redis.set(key, "1", nx=True, ex=600)
                if not acquired:
                    continue
            
            logger.info(f"Triggering cron task for {module_name} [{account}@{platform}] at {current_minute}")
            await self.trigger_single_task(module_name, account, platform)

    async def trigger_single_task(self, module_name: str, account: str, platform: str):
        task = TaskModel(
            account=account,
            platform=platform,
            module=[module_name],
            run_id=uuid7(),
            priority=Priority.NORMAL
        )
        
        if self.state.mq:
            try:
                await self.state.mq.publish_task(task)
            except Exception as e:
                logger.error(f"Failed to push cron task: {e}")
