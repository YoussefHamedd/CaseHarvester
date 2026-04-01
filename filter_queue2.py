#!/usr/bin/env python3
"""
Filter scraper queue: remove cases already in DB (either hyphenated or no-hyphen format).
"""
import json, redis, psycopg2

REDIS_URL = "redis://localhost:6379"
DB_URL = "postgresql://mjcs_user:mjcs_password@localhost:5432/mjcs"
QUEUE_KEY = "queue:scraper-queue"
BATCH = 5000

r = redis.from_url(REDIS_URL)
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

total = r.llen(QUEUE_KEY)
print(f"Queue size: {total}")

print("Reading queue...")
raw_items = r.lrange(QUEUE_KEY, 0, -1)
print(f"Read {len(raw_items)} items")

items = []
for raw in raw_items:
    try:
        obj = json.loads(raw)
        items.append((obj["case_number"], raw.decode()))
    except Exception:
        pass

case_numbers = [cn for cn, _ in items]
print(f"Parsed {len(case_numbers)} case numbers")

# Build both formats for each case number
# C-24-CV-25-002287 -> C24CV25002287
def no_hyphen(cn):
    return cn.replace('-', '')

all_variants = list(set(case_numbers + [no_hyphen(cn) for cn in case_numbers]))
print(f"Checking {len(all_variants)} case number variants against DB...")

already_known = set()

for i in range(0, len(all_variants), BATCH):
    batch = all_variants[i:i+BATCH]
    # Check cases table
    cur.execute("SELECT case_number FROM cases WHERE case_number = ANY(%s)", (batch,))
    for row in cur.fetchall():
        already_known.add(row[0])
    # Check scrape_versions table
    cur.execute("SELECT DISTINCT case_number FROM scrape_versions WHERE case_number = ANY(%s)", (batch,))
    for row in cur.fetchall():
        already_known.add(row[0])
    print(f"  Checked {min(i+BATCH, len(all_variants))}/{len(all_variants)}, known so far: {len(already_known)}")

print(f"\nFound {len(already_known)} known variants in DB")

# Mark queue items to remove if either format is known
def is_known(cn):
    return cn in already_known or no_hyphen(cn) in already_known

keep = [(cn, raw) for cn, raw in items if not is_known(cn)]
remove_count = len(items) - len(keep)

print(f"Removing {remove_count} already-known items")
print(f"Keeping {len(keep)} genuinely new items")

if remove_count == 0:
    print("Nothing to remove, queue unchanged.")
else:
    print("Rebuilding queue...")
    pipe = r.pipeline()
    pipe.delete(QUEUE_KEY)
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
