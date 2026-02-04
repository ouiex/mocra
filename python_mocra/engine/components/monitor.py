import asyncio
import logging
import psutil
import time
from typing import Optional

from common.state import get_state

logger = logging.getLogger(__name__)

class SystemMonitor:
    """
    Monitor system resources (CPU, Memory) and logs/exports them.
    Corresponds to engine/src/monitor.rs
    """
    def __init__(self, interval: int = 60):
        self.interval = interval
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if self._running:
            return
        
        self._running = True
        logger.info(f"SystemMonitor started with interval {self.interval}s")
        self._task = asyncio.create_task(self._monitor_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _monitor_loop(self):
        while self._running:
            try:
                # CPU Usage (since last call)
                cpu_percent = psutil.cpu_percent(interval=None)
                
                # Memory Usage
                mem = psutil.virtual_memory()
                mem_percent = mem.percent
                mem_used_mb = mem.used / (1024 * 1024)
                mem_total_mb = mem.total / (1024 * 1024)
                
                # Swap
                swap = psutil.swap_memory()
                
                # Log metrics
                logger.info(
                    f"System Metrics: CPU: {cpu_percent:.1f}%, "
                    f"Mem: {mem_percent:.1f}% ({mem_used_mb:.0f}/{mem_total_mb:.0f} MB)"
                )
                
                # In Rust we export to Prometheus here. 
                # For Python demo, we just log.
                
            except Exception as e:
                logger.error(f"Error in SystemMonitor: {e}")
            
            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
