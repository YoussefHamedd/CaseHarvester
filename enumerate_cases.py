#!/usr/bin/env python3
"""
Enumerate all C-{county}-CV-{YY}-{seq} case numbers for 2024-2026
that are NOT already in our cases table, and push them to the scraper queue.
"""
import json, redis, os, sys
import psycopg2

DB_URL = "postgresql://case_explorer:Wt1Wc3yny9XHhChCktVj@172.18.0.1:5432/mjcs"
REDIS_URL = "redis://localhost:6379/0"
SCRAPER_QUEUE = "queue:scraper-queue"

# max sequences per (county, year) — add 30% buffer to catch new cases
MAX_SEQS = {
    ("01","24"):380,  ("01","25"):620,  ("01","26"):100,
    ("02","24"):4200, ("02","25"):4800, ("02","26"):900,
    ("03","24"):6300, ("03","25"):8000, ("03","26"):1900,
    ("04","24"):750,  ("04","25"):680,
    ("05","24"):250,  ("05","25"):170,  ("05","26"):45,
    ("06","24"):490,  ("06","25"):710,  ("06","26"):110,
    ("07","24"):950,  ("07","25"):700,  ("07","26"):15,
    ("08","24"):1020, ("08","25"):1600, ("08","26"):420,
    ("09","24"):545,  ("09","25"):380,  ("09","26"):110,
    ("10","24"):1130, ("10","25"):1380, ("10","26"):250,
    ("11","24"):150,  ("11","25"):100,  ("11","26"):15,
    ("12","24"):1510, ("12","25"):1460, ("12","26"):420,
    ("13","24"):1800, ("13","25"):1560, ("13","26"):420,
    ("14","24"):270,  ("14","25"):300,  ("14","26"):10,
    ("15","24"):9000, ("15","25"):12000,("15","26"):2500,
    ("16","24"):8100, ("16","25"):9600, ("16","26"):2700,
    ("17","24"):290,  ("17","25"):300,  ("17","26"):65,
    ("18","24"):950,  ("18","25"):920,  ("18","26"):70,
    ("19","24"):115,  ("19","25"):270,  ("19","26"):10,
    ("20","24"):125,  ("20","25"):445,  ("20","26"):190,
    ("21","24"):680,  ("21","25"):875,  ("21","26"):250,
    ("22","24"):685,  ("22","25"):600,  ("22","26"):80,
    ("23","24"):290,  ("23","25"):485,  ("23","26"):100,
    ("24","24"):6200, ("24","25"):14000,("24","26"):3500,
}

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    r = redis.from_url(REDIS_URL)

    # Load all known case numbers for 2024-2026 into a set
    print("Loading known case numbers from DB...")
    cur.execute("""
        SELECT case_number FROM cases
        WHERE case_number ~ '^C-[0-9]+-CV-(24|25|26)-'
    """)
    known = set(row[0] for row in cur.fetchall())
    print(f"  {len(known):,} known cases")

    # Generate all candidate case numbers
    pushed = 0
    pipe = r.pipeline()
    batch = []

    for (county, yr), max_seq in sorted(MAX_SEQS.items()):
        for seq in range(1, max_seq + 1):
            cn = f"C-{county}-CV-{yr}-{seq:06d}"
            if cn not in known:
                batch.append(json.dumps({"case_number": cn, "detail_loc": "MJCS2"}))
                if len(batch) >= 5000:
                    for item in batch:
                        pipe.rpush(SCRAPER_QUEUE, item)
                    pipe.execute()
                    pushed += len(batch)
                    print(f"  Pushed {pushed:,} so far...")
                    batch = []

    if batch:
        for item in batch:
            pipe.rpush(SCRAPER_QUEUE, item)
        pipe.execute()
        pushed += len(batch)

    print(f"\nDone. Pushed {pushed:,} case numbers to scraper queue.")
    conn.close()

if __name__ == "__main__":
    main()
