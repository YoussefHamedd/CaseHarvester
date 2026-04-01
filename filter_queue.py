#!/usr/bin/env python3
"""
Filter scraper queue: remove case numbers already in cases table or scrape_versions.
Keeps only cases that haven't been scraped yet.
"""
import json
import redis
import psycopg2
import os

REDIS_URL = "redis://localhost:6379"
DB_URL = "postgresql://mjcs_user:mjcs_password@localhost:5432/mjcs"
QUEUE_KEY = "queue:scraper-queue"
BATCH = 5000

r = redis.from_url(REDIS_URL)
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

total = r.llen(QUEUE_KEY)
print(f"Queue size: {total}")

# Dump entire queue
print("Reading queue...")
raw_items = r.lrange(QUEUE_KEY, 0, -1)
print(f"Read {len(raw_items)} items")

# Parse case numbers
items = []
for raw in raw_items:
    try:
        obj = json.loads(raw)
        items.append((obj["case_number"], raw.decode()))
    except Exception:
        pass

case_numbers = [cn for cn, _ in items]
print(f"Parsed {len(case_numbers)} case numbers")

# Find which are already in cases table OR scrape_versions
print("Querying DB for already-known cases...")
already_known = set()

for i in range(0, len(case_numbers), BATCH):
    batch = case_numbers[i:i+BATCH]
    cur.execute(
        "SELECT case_number FROM cases WHERE case_number = ANY(%s)",
        (batch,)
    )
    for row in cur.fetchall():
        already_known.add(row[0])

    cur.execute(
        "SELECT DISTINCT case_number FROM scrape_versions WHERE case_number = ANY(%s)",
        (batch,)
    )
    for row in cur.fetchall():
        already_known.add(row[0])

    print(f"  Checked {min(i+BATCH, len(case_numbers))}/{len(case_numbers)}, known so far: {len(already_known)}")

print(f"\nAlready in DB: {len(already_known)}")
print(f"New (not in DB): {len(case_numbers) - len(already_known)}")

# Build filtered list preserving order
keep = [(cn, raw) for cn, raw in items if cn not in already_known]
print(f"Keeping {len(keep)} items")

# Rebuild queue atomically
print("Rebuilding queue...")
pipe = r.pipeline()
pipe.delete(QUEUE_KEY)
# Push in chunks to avoid huge pipelines
chunk = 500
for i in range(0, len(keep), chunk):
    batch_raw = [raw for _, raw in keep[i:i+chunk]]
    pipe.rpush(QUEUE_KEY, *batch_raw)
    if (i // chunk) % 20 == 0:
        pipe.execute()
        pipe = r.pipeline()
        print(f"  Pushed {min(i+chunk, len(keep))}/{len(keep)}")
pipe.execute()

final = r.llen(QUEUE_KEY)
print(f"\nDone. Queue: {total} → {final} (removed {total - final})")
conn.close()
