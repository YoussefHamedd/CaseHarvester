#!/usr/bin/env python3
"""
FC Enumeration Spider - pass-based approach.
Pass 1: try all sequences, skip blocked ones
Pass 2+: retry only the blocked sequences
This ensures forward progress regardless of block rate.
Uses exact same headers/warmup as working scraper.
"""
import sys, os, json, time, random, re, signal, threading
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, '/opt/casetools/CaseHarvester')
_ENV = '/opt/casetools/CaseHarvester/env/production.env'
with open(_ENV) as f:
    for l in f:
        l = l.strip()
        if l and not l.startswith('#') and '=' in l:
            k, v = l.split('=', 1); os.environ.setdefault(k.strip(), v.strip())

from curl_cffi import requests as cffi_requests
import psycopg2, redis as redis_lib

BASE_URL   = 'https://casesearch.courts.state.md.us'
DETAIL_URL = f'{BASE_URL}/api-casedetails/v1/public/cases'
WARMUP_URL = f'{BASE_URL}/casesearch'

WARMUP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
}
DETAIL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://casesearch.courts.state.md.us/casesearch',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

PROFILES = ['firefox135', 'firefox133', 'firefox135', 'firefox133']
_pidx = 0
def _next_profile():
    global _pidx; p = PROFILES[_pidx % len(PROFILES)]; _pidx += 1; return p

_pu = os.getenv('PROXY_USERNAME',''); _pp = os.getenv('PROXY_PASSWORD','')
_ph = os.getenv('PROXY_HOST','');    _po = os.getenv('PROXY_PORT','80')
PROXIES = {'https': f'http://{_pu}:{_pp}@{_ph}:{_po}'} if _ph else {}

QUEUE_KEY     = 'queue:scraper-queue'
WORKERS       = 10
PROGRESS_FILE = '/opt/casetools/CaseHarvester/fc_enum_progress.json'

TARGET_TYPES = {
    'Foreclosure','Foreclosure - Residential','Foreclosure - Commercial',
    'Foreclosure - In Rem.','Foreclosure - In Rem','Right of Redemption'
}

PLAN = [
    ("24","25",14000),("24","26",4000),
    ("16","25",10000),("16","26",3000),
    ("15","25",12500),("15","26",3000),
    ("03","25",8500), ("03","26",2500),
    ("02","25",7000), ("02","26",2000),
    ("13","25",5000), ("13","26",1500),
    ("05","25",5500), ("05","26",1500),
    ("10","25",4500), ("10","26",1200),
    ("06","25",2800), ("06","26",800),
    ("04","25",4000), ("04","26",1200),
    ("18","25",1800), ("18","26",600),
    ("21","25",3500), ("21","26",1000),
    ("07","25",2500), ("07","26",800),
    ("22","25",2200), ("22","26",700),
    ("17","25",1200), ("17","26",400),
    ("20","25",1000), ("20","26",300),
    ("01","25",2000), ("01","26",600),
    ("11","25",800),  ("11","26",250),
    ("09","25",900),  ("09","26",280),
    ("08","25",900),  ("08","26",280),
    ("14","25",600),  ("14","26",200),
    ("19","25",600),  ("19","26",200),
    ("23","25",1200), ("23","26",400),
    ("12","25",1500), ("12","26",400),
]

progress  = {}
lock      = threading.Lock()
stop_flag = False
total_found = 0
total_checked = 0
current_segment = ""
start_time = 0

def load_state():
    global total_found, total_checked, start_time
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            d = json.load(f)
            progress.update(d.get('progress', {}))
            total_found   = d.get('total_found', 0)
            total_checked = d.get('total_checked', 0)
    start_time = start_time or time.time()
    print(f'Resumed: {len(progress)} segments, {total_found} found so far', flush=True)

def _write_combined():
    try:
        combined = {'total_found': 0, 'total_checked': 0, 'current_segment': current_segment,
                    'start_time': start_time, 'running': True}
        base = '/opt/casetools/CaseHarvester/fc_enum_progress'
        for i in [1, 2]:
            fn = f'{base}_inst{i}.json'
            if os.path.exists(fn):
                with open(fn) as f2:
                    d = json.load(f2)
                combined['total_found']   += d.get('total_found', 0)
                combined['total_checked'] += d.get('total_checked', 0)
        with open(f'{base}.json', 'w') as f2:
            json.dump(combined, f2)
    except Exception:
        pass

def save_state():
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({
            'progress': progress,
            'total_found': total_found,
            'total_checked': total_checked,
            'current_segment': current_segment,
            'start_time': start_time,
        }, f)
    _write_combined()

def handle_stop(sig, frame):
    global stop_flag
    stop_flag = True; save_state(); sys.exit(0)
signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

_db_conn = None
_redis_conn = None
_db_lock = threading.Lock()

def _get_db():
    global _db_conn
    try:
        if _db_conn is None or _db_conn.closed: raise Exception()
        _db_conn.cursor().execute('SELECT 1')
    except Exception:
        _db_conn = psycopg2.connect(os.environ['MJCS_DATABASE_URL'])
    return _db_conn

def _get_redis():
    global _redis_conn
    if _redis_conn is None:
        _redis_conn = redis_lib.from_url(os.getenv('REDIS_URL','redis://localhost:6379'))
    return _redis_conn

def insert_one(c):
    with _db_lock:
        conn = _get_db()
        r    = _get_redis()
        cur  = conn.cursor()
    cn = c['case_number']; fd_str = c.get('filing_date','')
    fd = None
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', fd_str or '')
    if m:
        try: fd = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except: pass
    if not fd:
        m2 = re.match(r'^C-\d+-CV-(\d{2})-', cn)
        if m2: fd = date(2000+int(m2.group(1)), 1, 1)
    cur.execute("""
        INSERT INTO cases (case_number,case_type,filing_date,filing_date_original,caption,court,detail_loc,active)
        VALUES (%s,%s,%s,%s,%s,%s,'MJCS2',true)
        ON CONFLICT (case_number) DO NOTHING
    """, (cn, c.get('case_type',''), fd, fd_str, c.get('caption',''), c.get('court','')))
    inserted = cur.rowcount
    cur.execute("SELECT 1 FROM scrape_versions WHERE case_number=%s LIMIT 1", (cn,))
    queued = 0
    if not cur.fetchone():
        r.rpush(QUEUE_KEY, json.dumps({"case_number": cn, "detail_loc": "MJCS2"}))
        queued = 1
        conn.commit(); cur.close()
    return inserted, queued

def fetch_case(case_num):
    time.sleep(random.uniform(0.5, 1.5))
    session = cffi_requests.Session(impersonate=_next_profile(), proxies=PROXIES)
    try:
        session.get(WARMUP_URL, headers=WARMUP_HEADERS, timeout=(10, 20))
        time.sleep(random.uniform(0.3, 0.8))
    except Exception:
        pass
    try:
        r = session.get(f'{DETAIL_URL}/{case_num}', headers=DETAIL_HEADERS, timeout=(10, 30))
    except Exception:
        return 'blocked'
    if r.status_code == 404: return 'notfound'
    if r.status_code == 403:
        time.sleep(random.uniform(3.0, 6.0))
        return 'blocked'
    if r.status_code == 200 and r.text.startswith('{'):
        d = r.json().get('caseDetail', {})
        ct = d.get('caseType', '')
        if ct in TARGET_TYPES:
            return {'case_number': case_num, 'case_type': ct,
                    'filing_date': d.get('filedDate',''),
                    'caption': d.get('caseTitle',''), 'court': d.get('courtSystem','')}
        return 'skip'
    return 'blocked'

def run_segment(county, yy, max_seq):
    global current_segment, total_checked, start_time
    import time as _t
    if not start_time: start_time = _t.time()
    current_segment = f'{county}/{yy}'
    global total_found
    key   = f'{county}-{yy}'
    seg_done = progress.get(key, {})
    if seg_done == 'done':
        print(f'  Skipping {key} (complete)', flush=True)
        return

    done_seqs    = set(seg_done.get('done', []))
    empty_seqs   = set(seg_done.get('empty', []))
    all_seqs     = set(range(1, max_seq + 1))
    todo         = sorted(all_seqs - done_seqs - empty_seqs)

    if not todo:
        print(f'  {key} fully resolved', flush=True)
        progress[key] = 'done'; save_state(); return

    print(f'\n▶ {key}: {len(todo)} sequences remaining ({len(done_seqs)} done, {len(empty_seqs)} empty)', flush=True)

    pass_num = 0
    while todo and not stop_flag:
        pass_num += 1
        blocked_this_pass = []
        print(f'  Pass {pass_num}: {len(todo)} sequences to check', flush=True)

        for i in range(0, len(todo), WORKERS):
            if stop_flag: break
            wave = todo[i:i + WORKERS]
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                futures = {ex.submit(fetch_case, f'C-{county}-CV-{yy}-{s:06d}'): s for s in wave}
                for fut in as_completed(futures):
                    s = futures[fut]; res = fut.result()
                    total_checked += 1
                    if res == 'notfound':
                        empty_seqs.add(s)
                    elif res == 'blocked':
                        blocked_this_pass.append(s)
                    elif res == 'skip':
                        done_seqs.add(s)
                    elif isinstance(res, dict):
                        done_seqs.add(s); total_found += 1
                        ins, q = insert_one(res)
                        print(f'  ✅ {res["case_number"]} — {res["case_type"]} | ins={ins} q={q} | total={total_found}', flush=True)

            # Save progress every 200 seqs
            if (i // WORKERS) % 10 == 0:
                with lock:
                    progress[key] = {'done': list(done_seqs), 'empty': list(empty_seqs)}
                save_state()

        # Next pass only retries blocked ones
        todo = sorted(blocked_this_pass)
        block_rate = len(blocked_this_pass) / max(len(all_seqs - empty_seqs), 1) * 100
        print(f'  Pass {pass_num} done. Blocked: {len(blocked_this_pass)} ({block_rate:.0f}%) | Found so far: {total_found}', flush=True)

        if todo:
            wait = min(30 * pass_num, 120)
            print(f'  Waiting {wait}s before pass {pass_num+1}...', flush=True)
            time.sleep(wait)

    with lock:
        progress[key] = {'done': list(done_seqs), 'empty': list(empty_seqs)}
    save_state()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--instance', type=int, default=1, choices=[1, 2])
    args = parser.parse_args()
    inst = args.instance
    mid = len(PLAN) // 2
    my_plan = PLAN[:mid] if inst == 1 else PLAN[mid:]
    PROGRESS_FILE = PROGRESS_FILE.replace('.json', f'_inst{inst}.json')
    print(f'FC Enum Spider — instance {inst} | {len(my_plan)} segments | {WORKERS} workers', flush=True)
    load_state()
    for county, yy, max_seq in my_plan:
        if stop_flag: break
        run_segment(county, yy, max_seq)
    print(f'\nDone. Total FC found: {total_found}', flush=True)
