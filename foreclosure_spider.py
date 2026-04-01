"""
Foreclosure Spider — Combined Person + Business Edition
- Person search: known MD substitute trustee last names
- Business search: known MD mortgage servicers & law firms
- Splits date windows on 600-result cap
- Saves to cases + mjcs2, pushes new cases to scraper queue
- Queue: queue:fc-spider-queue
"""
import json, os, logging, random, time, sys
from datetime import datetime, timedelta, time as dt_time

def _to_midnight(dt):
    """Normalize a datetime to midnight (date-only precision). Prevents sub-day
    time components from causing rs < re on same-calendar-day windows."""
    return datetime.combine(dt.date(), dt_time.min)

_ENV_FILE = os.path.join(os.path.dirname(__file__), "env", "production.env")
if os.path.exists(_ENV_FILE):
    with open(_ENV_FILE) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

import redis, trio
from sqlalchemy import create_engine, text
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('fc_spider')

REDIS_URL     = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
QUEUE_KEY     = 'queue:fc-spider-queue'
SCRAPER_QUEUE = 'queue:scraper-queue'
CONCURRENCY   = int(os.getenv('SPIDER_CONCURRENCY', '4'))
BASE_URL      = 'https://casesearch.courts.state.md.us'
SEARCH_URL    = f'{BASE_URL}/api-caselist/v1/cases'
MAX_RESULTS   = 600
DAYS_PER_WIN  = 16

TARGET_TYPES = {
    'Foreclosure', 'Foreclosure - Residential',
    'Foreclosure - Commercial', 'Foreclosure - In Rem.',
    'Right of Redemption',
}

# ── Known MD substitute trustees (Person search by lastName) ──────────────────
TRUSTEE_LAST_NAMES = [
    'ANSELL', 'BRENNER', 'WARD', 'FERGUSON', 'CLARKE', 'YACKO',
    'SOLOMON', 'DIPIETRO', 'NADEL', 'THEOLOGOU', 'WITTSTADT',
    'SAVAGE', 'TAYLOR', 'WILLIAMSON', 'DRISCOLL', 'PIEL',
    'LIPINSKI', 'KIEFER', 'GARTNER', 'ROSENBERG', 'FRIEDMAN',
    'JONES', 'LYNN', 'BROWN', 'MURPHY', 'ROBINS', 'WHITE',
    'ALBA', 'GEESING', 'BIERMAN', 'COHN', 'GOLDBERG',
]

# ── Known MD mortgage servicers & law firms (Business search) ─────────────────
BUSINESS_NAMES = [
    # Servicers
    'PENNYMAC', 'CARRINGTON', 'PLANET HOME', 'NEWREZ', 'PHH',
    'NATIONSTAR', 'MR COOPER', 'OCWEN', 'SELENE', 'AMERIHOME',
    'SPECIALIZED LOAN', 'RUSHMORE', 'LAKEVIEW', 'FREEDOM MORTGAGE',
    'ROUNDPOINT', 'FLAGSTAR', 'HOMEBRIDGE', 'MIDFIRST', 'CASCADE',
    'BAYVIEW', 'DITECH', 'GREEN TREE', 'WALTER', 'LOANCARE',
    'CAPITAL COVE', 'M&T BANK', 'TRUIST',
    # Banks filing directly
    'WELLS FARGO', 'BANK OF AMERICA', 'DEUTSCHE BANK', 'US BANK',
    'JP MORGAN', 'CITIBANK', 'PNC BANK', 'NAVY FEDERAL',
    'SUNTRUST', 'REGIONS BANK', 'FIFTH THIRD', 'CALIBER',
    # Law firms
    'BWW', 'MCCABE', 'SHAPIRO', 'LOGS', 'SAMUEL WHITE',
    # Tax lien / tax sale investors
    'GREYMORR', 'THORNTON MELLON', 'MARYLAND CAPITAL', 'TREASURED LANDS',
    'MAYOR AND CITY', 'STEPHEN SCOTT',
]

SEARCH_HEADERS = {
    'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0',
    'Accept':          'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type':    'application/json',
    'Origin':          BASE_URL,
    'Referer':         f'{BASE_URL}/casesearch/inquiry-search',
    'Sec-Fetch-Dest':  'empty',
    'Sec-Fetch-Mode':  'cors',
    'Sec-Fetch-Site':  'same-origin',
}

# ── Redis ─────────────────────────────────────────────────────────────────────
_redis = None
def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.from_url(REDIS_URL)
    return _redis

def push_items(items):
    if items:
        get_redis().rpush(QUEUE_KEY, *items)

def pop_items(n=10):
    r = get_redis()
    pipe = r.pipeline()
    for _ in range(n):
        pipe.lpop(QUEUE_KEY)
    return [x for x in pipe.execute() if x]

def qlen():
    return get_redis().llen(QUEUE_KEY)

def push_to_scraper(case_numbers):
    if not case_numbers:
        return
    r = get_redis()
    pipe = r.pipeline()
    for cn in case_numbers:
        pipe.rpush(SCRAPER_QUEUE, json.dumps({'case_number': cn, 'detail_loc': 'MJCS2'}))
    pipe.execute()

# ── DB ────────────────────────────────────────────────────────────────────────
_engine = None
def get_engine():
    global _engine
    if _engine is None:
        url = os.getenv('MJCS_DATABASE_URL', 'postgresql://mjcs_user:mjcs_password@localhost:5432/mjcs')
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine

def save_cases(cases):
    if not cases:
        return 0
    sql_cases = text("""
        INSERT INTO cases (case_number, court, case_type, filing_date,
                           filing_date_original, caption, detail_loc, active, scrape_exempt)
        VALUES (:case_number, :court_name, :case_type, :filing_date,
                :filing_date_str, :case_title, 'MJCS2', false, false)
        ON CONFLICT (case_number) DO NOTHING
    """)
    sql_mjcs2 = text("""
        INSERT INTO mjcs2 (case_number, court_system, case_category, case_type,
                           case_title, filing_date, court_name)
        VALUES (:case_number, :court_system, :case_category, :case_type,
                :case_title, :filing_date, :court_name)
        ON CONFLICT (case_number) DO NOTHING
    """)
    inserted = 0
    new_case_numbers = []
    with get_engine().begin() as conn:
        for c in cases:
            conn.execute(sql_cases, c)
            rows = conn.execute(sql_mjcs2, c).rowcount
            if rows > 0:
                new_case_numbers.append(c['case_number'])
            inserted += rows
    push_to_scraper(new_case_numbers)
    return inserted

# ── Curl pool ─────────────────────────────────────────────────────────────────
_PROFILES = ['firefox133', 'firefox135', 'firefox144', 'chrome120', 'chrome131']

class CurlPool:
    def __init__(self, size):
        self.size = size
        self._sc = self._rc = None
        self._cnt   = {}   # total uses per session id
        self._fails = {}   # consecutive 403/error count per session id
        self._idx = 0
        pu   = os.getenv('PROXY_USERNAME', '')
        pp   = os.getenv('PROXY_PASSWORD', '')
        ph   = os.getenv('PROXY_HOST', '')
        port = os.getenv('PROXY_PORT', '80')
        self._px = {'https': f'http://{pu}:{pp}@{ph}:{port}'} if ph else {}

    def _new(self):
        p = _PROFILES[self._idx % len(_PROFILES)]
        self._idx += 1
        s = cffi_requests.Session(impersonate=p, proxies=self._px)
        self._cnt[id(s)]   = 0
        self._fails[id(s)] = 0
        return s

    async def start(self):
        self._sc, self._rc = trio.open_memory_channel(self.size)
        for _ in range(self.size):
            await self._sc.send(self._new())

    async def get(self):
        return await self._rc.receive()

    def put(self, s, blocked=False):
        """Return session to pool. If blocked=True, count the failure;
        discard and replace after 2 consecutive blocks."""
        sid = id(s)
        self._cnt[sid]   = self._cnt.get(sid, 0) + 1
        if blocked:
            self._fails[sid] = self._fails.get(sid, 0) + 1
        else:
            self._fails[sid] = 0  # reset on success

        # Replace if: 2+ consecutive blocks, OR 25 normal uses
        if self._fails.get(sid, 0) >= 2 or self._cnt.get(sid, 0) >= 25:
            logger.debug(f'Recycling session (uses={self._cnt.get(sid)}, fails={self._fails.get(sid)})')
            s = self._new()

        try:
            self._sc.send_nowait(s)
        except trio.WouldBlock:
            pass

# ── Seeder ────────────────────────────────────────────────────────────────────
def seed_queue(start_date, end_date):
    slices = []
    current = start_date
    while current <= end_date:
        win_end = min(current + timedelta(DAYS_PER_WIN - 1), end_date)
        rs = current.isoformat()
        re = win_end.isoformat()
        # Person searches
        for name in TRUSTEE_LAST_NAMES:
            slices.append(json.dumps({
                'search_type': 'person',
                'range_start_date': rs,
                'range_end_date':   re,
                'search_term':      name,
            }))
        # Business searches
        for biz in BUSINESS_NAMES:
            slices.append(json.dumps({
                'search_type': 'business',
                'range_start_date': rs,
                'range_end_date':   re,
                'search_term':      biz,
            }))
        current = win_end + timedelta(1)

    logger.info(f'Seeding {len(slices):,} items ({len(TRUSTEE_LAST_NAMES)} person + {len(BUSINESS_NAMES)} business) → {QUEUE_KEY}')
    push_items(slices)
    logger.info('Seeding done.')

# ── Search node ───────────────────────────────────────────────────────────────
class SearchNode:
    def __init__(self, pool, rs, re, search_type, search_term):
        self.pool        = pool
        self.rs          = rs
        self.re          = re
        self.search_type = search_type
        self.search_term = search_term

    @property
    def id(self):
        return f'{self.rs:%Y-%m-%d}/{self.re:%Y-%m-%d}/{self.search_type[:3]}/{self.search_term}'

    async def run(self):
        session = await self.pool.get()
        blocked = False
        try:
            result = await trio.to_thread.run_sync(lambda: self._search(session))
            blocked = (result is None and self._last_was_blocked)
        finally:
            self.pool.put(session, blocked=blocked)
        if result is None:
            return
        total, fc_count = result
        # Only split if: total hit the API cap AND we actually found FC cases
        # (prevents infinite expansion on non-FC searches like CITIBANK civil cases)
        if total >= MAX_RESULTS and fc_count > 0 and self.rs < self.re:
            self._split()

    def _search(self, session):
        self._last_was_blocked = False
        if self.search_type == 'person':
            body = {
                'searchPartyType': 'Person',
                'lastName':        self.search_term,
                'firstName':       '',
                'middleName':      '',
                'businessName':    '',
                'startDate':       self.rs.strftime('%-m/%-d/%Y'),
                'endDate':         self.re.strftime('%-m/%-d/%Y'),
                'caseType':        '',
            }
        else:
            body = {
                'searchPartyType': 'Business',
                'businessName':    self.search_term,
                'firstName':       '',
                'lastName':        '',
                'middleName':      '',
                'startDate':       self.rs.strftime('%-m/%-d/%Y'),
                'endDate':         self.re.strftime('%-m/%-d/%Y'),
                'caseType':        '',
            }

        time.sleep(random.uniform(2.0, 4.0))
        try:
            r = session.post(SEARCH_URL, headers=SEARCH_HEADERS,
                             data=json.dumps(body), timeout=90)
        except Exception as e:
            logger.warning(f'Error {self.id}: {e}')
            self._last_was_blocked = True
            self._requeue()  # always retry — nothing is lost
            return None

        if r.status_code == 403:
            logger.warning(f'DataDome {self.id} — re-queuing')
            self._last_was_blocked = True
            self._requeue()  # always retry — nothing is lost
            return None
        if r.status_code == 400:
            return (0, 0)
        if r.status_code != 200:
            logger.warning(f'{r.status_code} for {self.id} — re-queuing')
            self._last_was_blocked = True
            self._requeue()  # always retry — nothing is lost
            return None

        try:
            data = json.loads(r.text)
        except Exception:
            return (0, 0)
        if not isinstance(data, list):
            return (0, 0)

        matches = [row for row in data if row.get('caseType', '') in TARGET_TYPES]
        if matches:
            to_save = []
            for row in matches:
                fd = None
                try:
                    fd = datetime.strptime(row.get('filingDate', ''), '%m/%d/%Y').date()
                except Exception:
                    pass
                cn = row.get('caseNumber', '').strip()
                if cn:
                    to_save.append({
                        'case_number':     cn,
                        'court_system':    'MDEC',
                        'case_category':   row.get('caseCategory', ''),
                        'case_type':       row.get('caseType', ''),
                        'case_title':      row.get('title', ''),
                        'filing_date':     fd,
                        'filing_date_str': row.get('filingDate', ''),
                        'court_name':      row.get('locationName', ''),
                    })
            inserted = save_cases(to_save)
            if inserted:
                logger.info(f'{self.id}: +{inserted} new | {len(matches)} FC | {len(data)} total')
            else:
                logger.info(f'{self.id}: {len(matches)} FC matched, all known | {len(data)} total')
        else:
            logger.debug(f'{self.id}: {len(data)} results, 0 FC types')

        return (len(data), len(matches))

    def _requeue(self):
        push_items([json.dumps({
            'search_type':      self.search_type,
            'range_start_date': self.rs.isoformat(),
            'range_end_date':   self.re.isoformat(),
            'search_term':      self.search_term,
        })])

    def _split(self):
        # Hard guard: never split a same-day or invalid window
        if self.rs >= self.re:
            logger.debug(f'Skip split {self.id} (same-day or inverted window)')
            return
        # Use date-only midpoint to avoid sub-day time drift
        rs_date = self.rs.date()
        re_date = self.re.date()
        days = (re_date - rs_date).days
        mid_date = rs_date + timedelta(days=days // 2)
        mid      = _to_midnight(datetime.combine(mid_date, dt_time.min))
        mid_next = _to_midnight(datetime.combine(mid_date + timedelta(1), dt_time.min))
        if mid_next > self.re:
            # Window is only 1 day wide after rounding — don't split
            logger.debug(f'Skip split {self.id} (1-day window, no further split possible)')
            return
        push_items([
            json.dumps({'search_type': self.search_type, 'search_term': self.search_term,
                        'range_start_date': self.rs.date().isoformat(), 'range_end_date': mid_date.isoformat()}),
            json.dumps({'search_type': self.search_type, 'search_term': self.search_term,
                        'range_start_date': (mid_date + timedelta(1)).isoformat(), 'range_end_date': re_date.isoformat()}),
        ])
        logger.info(f'Split {self.id} → 2 windows')

# ── Runner ────────────────────────────────────────────────────────────────────
class FCSpider:
    def __init__(self, concurrency):
        self.concurrency = concurrency

    async def _run(self):
        pool = CurlPool(self.concurrency)
        await pool.start()
        logger.info(f'FC spider (combined) starting concurrency={self.concurrency}')
        while True:
            items = pop_items(self.concurrency * 2)
            if not items:
                if qlen() == 0:
                    logger.info('Queue empty — done.')
                    break
                await trio.sleep(5)
                continue
            nodes = []
            for raw in items:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                try:
                    d = json.loads(raw)
                    rs = _to_midnight(datetime.fromisoformat(d['range_start_date']))
                    re = _to_midnight(datetime.fromisoformat(d['range_end_date']))
                    nodes.append(SearchNode(pool, rs, re, d['search_type'], d['search_term']))
                except Exception as ex:
                    logger.warning(f'Bad queue item: {ex}')
            async with trio.open_nursery() as nursery:
                for node in nodes:
                    nursery.start_soon(node.run)
        logger.info('Stopped.')

    def run(self):
        trio.run(self._run)

# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='FC Spider — Combined Edition')
    p.add_argument('--seed',        action='store_true')
    p.add_argument('--run',         action='store_true')
    p.add_argument('--queue-len',   action='store_true')
    p.add_argument('--concurrency', type=int, default=CONCURRENCY)
    p.add_argument('--start-date',  default='2024-07-01')
    p.add_argument('--end-date',    default=datetime.now().strftime('%Y-%m-%d'))
    args = p.parse_args()

    if args.queue_len:
        print(qlen())
    elif args.seed:
        sd = datetime.strptime(args.start_date, '%Y-%m-%d')
        ed = datetime.strptime(args.end_date,   '%Y-%m-%d')
        seed_queue(sd, ed)
    elif args.run:
        FCSpider(args.concurrency).run()
    else:
        p.print_help()
