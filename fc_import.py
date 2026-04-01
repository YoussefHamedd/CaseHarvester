#!/usr/bin/env python3
"""
Import FC/Redemption cases from browser-scraped JSON:
1. Insert basic info into `cases` table
2. Push case numbers to scraper queue (for full scrape later)
Usage: python3 fc_import.py fc_cases_XXXX.json
"""
import json, sys, re, redis
import psycopg2
from datetime import date

DB_URL    = "postgresql://mjcs_user:mjcs_password@localhost:5432/mjcs"
REDIS_URL = "redis://localhost:6379"
QUEUE_KEY = "queue:scraper-queue"

def parse_date(filing_date_str, case_number):
    if filing_date_str:
        s = str(filing_date_str).strip()
        m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s)
        if m:
            try: return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
            except: pass
        m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
        if m:
            try: return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except: pass
        m = re.match(r'^(\d{4})$', s)
        if m:
            try: return date(int(m.group(1)), 1, 1)
            except: pass
    # Derive year from case number: C-{county}-CV-{YY}-{seq}
    m = re.match(r'^C-\d+-CV-(\d{2})-', case_number or '')
    if m:
        return date(2000 + int(m.group(1)), 1, 1)
    return None

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/fc_import.json'
    print(f"Loading {path}...")
    cases = json.load(open(path))
    print(f"Loaded {len(cases)} cases")

    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()
    r    = redis.from_url(REDIS_URL)

    inserted_cases = 0
    skipped_cases  = 0
    queued         = 0

    for c in cases:
        cn = (c.get('case_number') or '').strip()
        if not cn:
            continue

        filing_date = parse_date(c.get('filing_date'), cn)
        case_type   = c.get('case_type', '')
        caption     = c.get('title', '')
        court       = c.get('court', '')

        # 1. Insert into cases table (basic info)
        cur.execute("""
            INSERT INTO cases (case_number, case_type, filing_date, filing_date_original, caption, court, detail_loc, active)
            VALUES (%s, %s, %s, %s, %s, %s, 'MJCS2', true)
            ON CONFLICT (case_number) DO NOTHING
        """, (cn, case_type, filing_date, str(c.get('filing_date') or ''), caption, court))

        if cur.rowcount > 0:
            inserted_cases += 1
        else:
            skipped_cases += 1

        # 2. Push to scraper queue (for full JSON fetch later)
        #    Only if not already in scrape_versions (i.e. not fully scraped yet)
        cur.execute("SELECT 1 FROM scrape_versions WHERE case_number = %s LIMIT 1", (cn,))
        if not cur.fetchone():
            r.rpush(QUEUE_KEY, json.dumps({"case_number": cn, "detail_loc": "MJCS2"}))
            queued += 1

        if (inserted_cases + skipped_cases) % 200 == 0:
            conn.commit()
            print(f"  ...cases inserted={inserted_cases} skipped={skipped_cases} queued={queued}")

    conn.commit()
    conn.close()

    print(f"\nDone.")
    print(f"  Cases inserted (new): {inserted_cases}")
    print(f"  Cases skipped (existing): {skipped_cases}")
    print(f"  Pushed to scraper queue: {queued}")
    print(f"  Scraper queue size: {r.llen(QUEUE_KEY)}")

if __name__ == '__main__':
    main()
