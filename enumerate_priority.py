#!/usr/bin/env python3
"""
Push case numbers in PRIORITY order — highest FC counties first.
"""
import json, redis
import psycopg2

DB_URL = "postgresql://case_explorer:Wt1Wc3yny9XHhChCktVj@172.18.0.1:5432/mjcs"
REDIS_URL = "redis://localhost:6379/0"
SCRAPER_QUEUE = "queue:scraper-queue"

# (county, year, max_seq) sorted by expected FC volume — big counties first
PRIORITY_LIST = [
    # Baltimore City - highest FC volume
    ("24","25",14000), ("24","24",6200), ("24","26",3500),
    # Prince George's
    ("16","25",9600), ("16","24",8100), ("16","26",2700),
    # Montgomery
    ("15","25",12000),("15","24",9000), ("15","26",2500),
    # Baltimore County
    ("03","25",8000), ("03","24",6300), ("03","26",1900),
    # Anne Arundel
    ("02","25",4800), ("02","24",4200), ("02","26",900),
    # Howard
    ("13","25",1560), ("13","24",1800),("13","26",420),
    # Harford
    ("12","25",1460), ("12","24",1510),("12","26",420),
    # Frederick
    ("10","25",1380), ("10","24",1130),("10","26",250),
    # Charles
    ("08","25",1600), ("08","24",1020),("08","26",420),
    # Cecil
    ("07","24",950),  ("07","25",700), ("07","26",15),
    # Wicomico
    ("22","24",685),  ("22","25",600), ("22","26",80),
    # Washington
    ("21","25",875),  ("21","24",680), ("21","26",250),
    # Carroll
    ("06","25",710),  ("06","24",490), ("06","26",110),
    # Calvert
    ("05","24",250),  ("05","25",170), ("05","26",45),
    # Dorchester
    ("09","24",545),  ("09","25",380), ("09","26",110),
    # St Marys
    ("18","25",920),  ("18","24",950), ("18","26",70),
    # Worcester
    ("23","25",485),  ("23","24",290), ("23","26",100),
    # Queen Annes
    ("17","25",300),  ("17","24",290), ("17","26",65),
    # Talbot
    ("20","25",445),  ("20","24",125), ("20","26",190),
    # Kent
    ("14","25",300),  ("14","24",270), ("14","26",10),
    # Allegany
    ("01","24",380),  ("01","25",620), ("01","26",100),
    # Garrett
    ("11","24",150),  ("11","25",100), ("11","26",15),
    # Somerset
    ("19","24",115),  ("19","25",270), ("19","26",10),
    # Caroline
    ("04","24",750),  ("04","25",680),
]

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    r = redis.from_url(REDIS_URL)

    print("Loading known case numbers...")
    cur.execute("SELECT case_number FROM cases WHERE case_number ~* 'C.?[0-9]+.?CV.?(24|25|26)'")
    known = set(row[0] for row in cur.fetchall())
    # Also add without-hyphen variants
    known_norm = set(cn.replace('-','').upper() for cn in known)
    print(f"  {len(known):,} known cases")

    pushed = 0
    pipe = r.pipeline()
    batch = []

    for (county, yr, max_seq) in PRIORITY_LIST:
        county_pushed = 0
        for seq in range(1, max_seq + 1):
            cn = f"C-{county}-CV-{yr}-{seq:06d}"
            cn_norm = cn.replace('-','').upper()
            if cn not in known and cn_norm not in known_norm:
                batch.append(json.dumps({"case_number": cn, "detail_loc": "MJCS2"}))
                county_pushed += 1
                if len(batch) >= 5000:
                    for item in batch:
                        pipe.rpush(SCRAPER_QUEUE, item)
                    pipe.execute()
                    pushed += len(batch)
                    batch = []
        print(f"  County {county} yr {yr}: {county_pushed} pushed (total {pushed:,})")

    if batch:
        for item in batch:
            pipe.rpush(SCRAPER_QUEUE, item)
        pipe.execute()
        pushed += len(batch)

    print(f"\nDone. Total pushed: {pushed:,}")
    conn.close()

if __name__ == "__main__":
    main()
