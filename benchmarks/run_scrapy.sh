#!/usr/bin/env bash
# scrapy-redis N-node benchmark (direct-item). N item URLs are pushed to a shared Redis queue;
# NODES workers consume them. Measures push -> all-done (worker boot excluded via warmup).
#
# Prereq: a no-auth Redis on $REDIS_PORT (default 6399), the mock on :8899, and a venv:
#     python3 -m venv venv && venv/bin/pip install -r requirements.txt
# Usage:  N=30000 NODES=3 ./run_scrapy.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
VENV="${VENV:-$HERE/venv/bin}"
PORT="${REDIS_PORT:-6399}"
RC="redis-cli -p $PORT"
N="${N:-30000}"; CONC="${CONC:-64}"; NODES="${NODES:-3}"
[ -x "$VENV/scrapy" ] || { echo "create venv first:  python3 -m venv venv && venv/bin/pip install -r requirements.txt"; exit 1; }

$RC flushall >/dev/null; $RC set bench:count 0 >/dev/null
pids=()
for i in $(seq 1 "$NODES"); do
  "$VENV/scrapy" runspider "$HERE/scrapy_spider.py" \
    -s SCHEDULER=scrapy_redis.scheduler.Scheduler \
    -s DUPEFILTER_CLASS=scrapy_redis.dupefilter.RFPDupeFilter \
    -s REDIS_URL="redis://127.0.0.1:$PORT" -s SCHEDULER_PERSIST=True \
    -s CONCURRENT_REQUESTS="$CONC" -s CONCURRENT_REQUESTS_PER_DOMAIN="$CONC" \
    -s DOWNLOAD_DELAY=0 -s RANDOMIZE_DOWNLOAD_DELAY=False \
    -s AUTOTHROTTLE_ENABLED=False -s ROBOTSTXT_OBEY=False \
    -s RETRY_ENABLED=False -s COOKIES_ENABLED=False -s REDIRECT_ENABLED=False \
    -s LOG_LEVEL=CRITICAL -s TELNETCONSOLE_ENABLED=False \
    >/dev/null 2>&1 &
  pids+=($!)
done
sleep 8   # worker warmup (NOT counted)

t0=$(python3 -c 'import time;print(time.time())')
"$VENV/python" -c "import redis; redis.Redis(port=$PORT).rpush('bench:start_urls', *[f'http://127.0.0.1:8899/item/{i}' for i in range($N)])"
while [ "$($RC get bench:count 2>/dev/null || echo 0)" -lt "$N" ]; do sleep 0.02; done
t1=$(python3 -c 'import time;print(time.time())')

kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true
python3 -c "print(f'[scrapy {$NODES}-node]  N=$N conc=$CONC  ->  {$t1-$t0:.3f}s   {$N/($t1-$t0):.0f} pages/s')"
