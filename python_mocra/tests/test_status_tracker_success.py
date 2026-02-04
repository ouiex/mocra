import os
import sys
import pytest

# ensure project root on path for imports
sys.path.append(os.getcwd())

from fakeredis.aioredis import FakeRedis

from common.status_tracker import StatusTracker, ErrorTrackerConfig
from errors.error_stats import ErrorCategory, ErrorSeverity


@pytest.mark.asyncio
async def test_status_tracker_success_decay():
    redis = FakeRedis(decode_responses=True)
    config = ErrorTrackerConfig(enable_success_decay=True, success_decay_amount=1, error_ttl=3600)
    tracker = StatusTracker(redis, config)

    request_id = "req-1"
    key = f"request:{request_id}:download"

    # simulate an error
    await tracker._increment_error(key, category=ErrorCategory.DOWNLOAD, severity=ErrorSeverity.MINOR)
    val = await redis.get(f"{key}:total_errors")
    assert int(val) == 1

    # record success should decay error count to 0
    await tracker.record_download_success(request_id)
    val_after = await redis.get(f"{key}:total_errors")
    assert int(val_after) == 0
