#!/bin/bash
# Spider runner script - called by launchd scheduler
# Runs a small batch of spider slices, then stops (to avoid rate limiting)

HARVESTER_DIR="/Users/owner/Desktop/CaseHarvester"
LOG_FILE="/Users/owner/Desktop/CaseHarvester/logs/spider.log"
VENV="$HARVESTER_DIR/venv/bin/python"

mkdir -p "$HARVESTER_DIR/logs"

echo "[$(date)] Starting spider run" >> "$LOG_FILE"

cd "$HARVESTER_DIR"

# Queue slices for the past week if queue is empty
"$VENV" - << 'PYEOF' >> "$LOG_FILE" 2>&1
import sys, json, redis
from datetime import datetime, timedelta
sys.path.insert(0, '.')
from mjcs.config import config
config.initialize_from_environment(environment='production')

r = redis.from_url('redis://localhost:6379/0')
queue_len = r.llen('queue:spider-queue')

if queue_len < 10:
    # Queue is nearly empty, add slices for last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    # Use a subset of 2-char prefixes to keep it manageable
    import string
    prefixes = [a+b for a in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' for b in 'ABCDE']
    count = 0
    for prefix in prefixes[:50]:  # 50 slices per run
        r.rpush('queue:spider-queue', json.dumps({
            'range_start_date': start_date.strftime('%Y-%m-%d'),
            'range_end_date': end_date.strftime('%Y-%m-%d'),
            'court': None,
            'site': None,
            'search_string': prefix,
        }))
        count += 1
    print(f"Queued {count} spider slices ({start_date.date()} to {end_date.date()})")
else:
    print(f"Queue already has {queue_len} items, skipping queue fill")
PYEOF

# Run spider for up to 5 minutes
echo "[$(date)] Running spider from queue..." >> "$LOG_FILE"
"$VENV" "$HARVESTER_DIR/harvester.py" \
    --environment production \
    spider --from-queue \
    >> "$LOG_FILE" 2>&1

echo "[$(date)] Spider run complete" >> "$LOG_FILE"
echo "---" >> "$LOG_FILE"
