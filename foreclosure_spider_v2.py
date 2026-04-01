"""
Foreclosure + Right of Redemption Spider v2
============================================
Strategy: Use Playwright to solve DataDome and get cookies,
then use curl_cffi with those cookies for fast JSON API calls.

Key improvements over v1:
  - Playwright solves DataDome CAPTCHA → extracts datadome cookie
  - curl_cffi uses that cookie for fast API requests
  - Single search per prefix (API ignores caseType, filter client-side)
  - Exponential backoff on DataDome blocks → re-solve via Playwright
  - Tracks completed slices to avoid re-doing work
"""
import json, os, logging, random, string, time, sys, asyncio, threading
import queue as thread_queue

# Load production env
_ENV_FILE = os.path.join(os.path.dirname(__file__), "env", "production.env")
if os.path.exists(_ENV_FILE):
    with open(_ENV_FILE) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

from datetime import datetime, timedelta

import redis, trio

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('fc_spider')

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_URL    = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
QUEUE_KEY    = 'queue:fc-spider-queue'
DONE_SET_KEY = 'fc-spider:done'
CONCURRENCY  = int(os.getenv('FC_SPIDER_CONCURRENCY', '2'))
BASE_URL     = 'https://casesearch.courts.state.md.us'
SEARCH_URL   = f'{BASE_URL}/api-caselist/v1/cases'
MAX_RESULTS  = 600
DAYS_PER_WIN = 16

TARGET_TYPES = {
    'Foreclosure',
    'Foreclosure - Residential',
    'Foreclosure - Commercial',
    'Foreclosure - In Rem.',
    'Right of Redemption',
}

FIRST_CHARS  = string.ascii_uppercase + string.digits

SEARCH_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/json',
    'Origin': BASE_URL,
    'Referer': f'{BASE_URL}/casesearch/inquiry-search',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

PROFILES = ['firefox135', 'firefox133', 'firefox135']
_profile_idx = 0
def _next_profile():
    global _profile_idx
    p = PROFILES[_profile_idx % len(PROFILES)]
    _profile_idx += 1
    return p

# ── Redis helpers ─────────────────────────────────────────────────────────────
_redis = None
def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.from_url(REDIS_URL)
    return _redis

def push_items(items):
    if not items:
        return
    r = get_redis()
    pipe = r.pipeline()
    for it in items:
        pipe.rpush(QUEUE_KEY, it)
    pipe.execute()

def pop_items(n=1):
    r = get_redis()
    pipe = r.pipeline()
    for _ in range(n):
        pipe.lpop(QUEUE_KEY)
    results = pipe.execute()
    return [x.decode() if isinstance(x, bytes) else x for x in results if x]

def qlen():
    return get_redis().llen(QUEUE_KEY)

def mark_done(slice_id):
    get_redis().sadd(DONE_SET_KEY, slice_id)

def is_done(slice_id):
    return get_redis().sismember(DONE_SET_KEY, slice_id)

def clear_done():
    get_redis().delete(DONE_SET_KEY)

# ── Database (raw SQL, no ORM dependency) ─────────────────────────────────────
from sqlalchemy import create_engine, text as sql_text

_engine = None
def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(os.environ['MJCS_DATABASE_URL'], future=True)
    return _engine

def save_cases(cases):
    """Insert new cases into DB, return list of newly inserted case_numbers."""
    if not cases:
        return []
    eng = get_engine()
    new_cns = []
    with eng.begin() as conn:
        for c in cases:
            result = conn.execute(sql_text("""
                INSERT INTO cases (case_number, court, case_type, filing_date,
                                   filing_date_original, caption, detail_loc, status)
                VALUES (:cn, :court, :ct, :fd, :fds, :cap, 'MJCS2', :status)
                ON CONFLICT (case_number) DO NOTHING
            """), {
                'cn':     c['case_number'],
                'court':  c.get('court_name', ''),
                'ct':     c.get('case_type', ''),
                'fd':     c.get('filing_date'),
                'fds':    c.get('filing_date_str', ''),
                'cap':    c.get('case_title', ''),
                'status': c.get('status', ''),
            })
            if result.rowcount > 0:
                new_cns.append(c['case_number'])
    return new_cns

def queue_for_scraping(case_numbers):
    """Push case numbers into the scraper queue."""
    if not case_numbers:
        return
    r = get_redis()
    pipe = r.pipeline()
    for cn in case_numbers:
        pipe.rpush('queue:scraper-queue', json.dumps({
            'case_number': cn,
            'detail_loc': 'MJCS2'
        }))
    pipe.execute()


# ── DataDome Cookie Solver (Playwright) ──────────────────────────────────────
COOKIE_FILE = '/tmp/datadome_cookies.json'
MJCS_HOME = f'{BASE_URL}/casesearch/'

class DataDomeSolver:
    """
    Uses Playwright (headless Chromium) to visit MJCS, solve DataDome,
    accept the disclaimer, and extract all cookies including the datadome cookie.
    These cookies are then shared with curl_cffi sessions.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._cookies = {}
        self._last_solve = 0
        self._solve_count = 0

    def _proxy_cfg(self):
        user = os.getenv('PROXY_USERNAME', 'oehwhsaf-rotate')
        pw   = os.getenv('PROXY_PASSWORD', 'w03c3ohqnnbp')
        host = os.getenv('PROXY_HOST',     'p.webshare.io')
        port = os.getenv('PROXY_PORT',     '80')
        return {
            'server':   f'http://{host}:{port}',
            'username': user,
            'password': pw,
        }

    def solve(self):
        """Synchronous: launch browser, solve DataDome, return cookies dict."""
        with self._lock:
            # Don't re-solve if we just solved recently
            if time.time() - self._last_solve < 30 and self._cookies:
                return self._cookies
            self._solve_count += 1
            logger.info(f'Solving DataDome via Playwright (attempt #{self._solve_count})...')
            try:
                cookies = asyncio.run(self._async_solve())
                self._cookies = cookies
                self._last_solve = time.time()
                # Save to file for other processes
                with open(COOKIE_FILE, 'w') as f:
                    json.dump(cookies, f)
                logger.info(f'DataDome solved! Got {len(cookies)} cookies')
                return cookies
            except Exception as e:
                logger.error(f'DataDome solve failed: {e}')
                return self._cookies  # Return stale cookies if available

    async def _async_solve(self):
        from playwright.async_api import async_playwright

        # Try direct connection first, fallback to proxy
        configs = [
            ('direct', None),
            ('proxy', self._proxy_cfg()),
        ]

        for label, proxy_cfg in configs:
            try:
                cookies = await self._try_solve_with(proxy_cfg, label)
                if cookies:
                    return cookies
                logger.warning(f'[{label}] Got 0 cookies, trying next...')
            except Exception as e:
                logger.warning(f'[{label}] Failed: {e}')
                continue

        logger.error('All DataDome solve attempts failed')
        return {}

    async def _try_solve_with(self, proxy_cfg, label):
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            launch_args = {
                'headless': True,
                'args': ['--no-sandbox', '--disable-setuid-sandbox',
                         '--disable-dev-shm-usage',
                         '--disable-blink-features=AutomationControlled'],
            }
            if proxy_cfg:
                launch_args['proxy'] = proxy_cfg

            browser = await pw.chromium.launch(**launch_args)
            context = await browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                viewport={'width': 1280, 'height': 800},
                locale='en-US',
            )
            await context.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            )

            page = await context.new_page()
            try:
                logger.info(f'[{label}] Navigating to MJCS...')
                await page.goto(MJCS_HOME, wait_until='networkidle', timeout=45000)
                await asyncio.sleep(5)

                content = await page.content()

                # Check for DataDome block
                if 'Access is temporarily restricted' in content or \
                   ('datadome' in content.lower() and 'captcha' in content.lower()):
                    logger.warning(f'[{label}] DataDome CAPTCHA — waiting 20s and retrying...')
                    await asyncio.sleep(20)
                    await page.reload(wait_until='networkidle')
                    await asyncio.sleep(5)
                    content = await page.content()
                    if 'Access is temporarily restricted' in content:
                        logger.warning(f'[{label}] Still blocked after retry')
                        await page.close()
                        await browser.close()
                        return {}

                # Accept disclaimer — try button first, then input
                for selector in [
                    'button:has-text("I Agree")',
                    'button:has-text("Agree")',
                    'input[name="disclaimer"]',
                    'button[type="submit"]',
                ]:
                    try:
                        el = page.locator(selector)
                        if await el.count() > 0:
                            await el.first.click()
                            await page.wait_for_load_state('networkidle', timeout=10000)
                            await asyncio.sleep(2)
                            logger.info(f'[{label}] Disclaimer accepted via {selector}')
                            break
                    except Exception:
                        continue

                # Extract cookies
                cookies_list = await context.cookies()
                cookies = {}
                for c in cookies_list:
                    domain = c.get('domain', '')
                    if 'courts.state.md.us' in domain or 'casesearch' in domain:
                        cookies[c['name']] = c['value']

                logger.info(f'[{label}] Extracted {len(cookies)} cookies: {list(cookies.keys())}')
                return cookies
            finally:
                await page.close()
                await browser.close()

    def get_cookies(self):
        """Return current cookies, solving if needed."""
        if not self._cookies:
            # Try loading from file first
            if os.path.exists(COOKIE_FILE):
                try:
                    with open(COOKIE_FILE) as f:
                        self._cookies = json.load(f)
                    self._last_solve = os.path.getmtime(COOKIE_FILE)
                except Exception:
                    pass
            if not self._cookies:
                self.solve()
        return self._cookies


# ── Seed queue ────────────────────────────────────────────────────────────────
def seed_queue(start_date, end_date):
    """Generate 1-char prefix search slices."""
    slices = []
    cur = start_date
    while cur <= end_date:
        win_end = min(cur + timedelta(DAYS_PER_WIN - 1), end_date)
        for c in FIRST_CHARS:
            slice_id = f'{cur:%Y-%m-%d}/{win_end:%Y-%m-%d}/{c}'
            if not is_done(slice_id):
                slices.append(json.dumps({
                    'range_start_date': cur.isoformat(),
                    'range_end_date':   win_end.isoformat(),
                    'search_string':    c,
                }))
        cur = win_end + timedelta(1)

    logger.info(f'Seeding {len(slices):,} items → {QUEUE_KEY}')
    push_items(slices)
    logger.info('Seeding done.')


# ── Search node ───────────────────────────────────────────────────────────────
class SearchNode:
    # Shared state
    _consecutive_blocks = 0

    def __init__(self, solver, rs, re, name):
        self.solver = solver
        self.rs     = rs
        self.re     = re
        self.name   = name

    @property
    def id(self):
        return f'{self.rs:%Y-%m-%d}/{self.re:%Y-%m-%d}/{self.name}'

    def search(self):
        """Synchronous search using curl_cffi + DataDome cookies."""
        if is_done(self.id):
            return

        # Backoff on consecutive blocks
        if SearchNode._consecutive_blocks > 0:
            backoff = min(30 * (2 ** (SearchNode._consecutive_blocks - 1)), 300)
            logger.info(f'Backoff {backoff}s (blocks: {SearchNode._consecutive_blocks})')
            time.sleep(backoff)

        # Get cookies (may trigger Playwright solve)
        cookies = self.solver.get_cookies()

        # Build proxy URL
        _puser = os.getenv('PROXY_USERNAME', '')
        _ppass = os.getenv('PROXY_PASSWORD', '')
        _phost = os.getenv('PROXY_HOST', '')
        _pport = os.getenv('PROXY_PORT', '80')
        proxies = {'https': f'http://{_puser}:{_ppass}@{_phost}:{_pport}'} if _phost else {}

        body = {
            'searchPartyType': 'Person',
            'lastName':        self.name,
            'firstName':       '',
            'middleName':      '',
            'businessName':    '',
            'startDate':       self.rs.strftime('%-m/%-d/%Y'),
            'endDate':         self.re.strftime('%-m/%-d/%Y'),
        }

        # Polite delay
        time.sleep(random.uniform(5.0, 12.0))

        try:
            session = cffi_requests.Session(
                impersonate=_next_profile(),
                proxies=proxies,
            )
            # Inject DataDome cookies
            for name, value in cookies.items():
                session.cookies.set(name, value, domain='casesearch.courts.state.md.us')

            r = session.post(
                SEARCH_URL,
                headers=SEARCH_HEADERS,
                data=json.dumps(body),
                timeout=(10, 90),
            )
        except Exception as e:
            logger.warning(f'Error {self.id}: {e}')
            self._requeue()
            return

        if r.status_code == 403:
            SearchNode._consecutive_blocks += 1
            logger.warning(f'DataDome {self.id} — block #{SearchNode._consecutive_blocks}')
            # Force re-solve on next attempt
            if SearchNode._consecutive_blocks >= 2:
                logger.info('Re-solving DataDome via Playwright...')
                self.solver.solve()
            self._requeue()
            return

        if r.status_code == 400:
            mark_done(self.id)
            return

        if r.status_code != 200:
            logger.warning(f'{r.status_code} for {self.id}')
            self._requeue()
            return

        # Success — reset block counter
        if SearchNode._consecutive_blocks > 0:
            logger.info(f'Success after {SearchNode._consecutive_blocks} blocks — reset')
            SearchNode._consecutive_blocks = 0

        try:
            data = json.loads(r.text)
        except Exception:
            logger.warning(f'{self.id}: JSON parse error')
            return

        if not isinstance(data, list):
            return

        # Filter to target types
        filtered = [row for row in data if row.get('caseType', '') in TARGET_TYPES]

        if filtered:
            to_save = []
            for row in filtered:
                fd = None
                try:
                    fd = datetime.strptime(row.get('filingDate', ''), '%m/%d/%Y').date()
                except Exception:
                    pass
                cn = row.get('caseNumber', '').strip()
                if cn:
                    to_save.append({
                        'case_number':   cn,
                        'court_name':    row.get('locationName', ''),
                        'case_type':     row.get('caseType', ''),
                        'case_title':    row.get('title', ''),
                        'filing_date':   fd,
                        'filing_date_str': row.get('filingDate', ''),
                        'status':        row.get('caseStatus', ''),
                    })

            new_cns = save_cases(to_save)
            if new_cns:
                queue_for_scraping(new_cns)
                logger.info(f'{self.id}: +{len(new_cns)} NEW | {len(filtered)} matched | {len(data)} total')
            else:
                logger.debug(f'{self.id}: {len(filtered)} matched, all known')
        else:
            logger.debug(f'{self.id}: {len(data)} results, 0 target types')

        # Expand if we hit the cap on filtered results
        if len(filtered) >= MAX_RESULTS:
            self._expand()
        else:
            mark_done(self.id)

    def _requeue(self):
        push_items([json.dumps({
            'range_start_date': self.rs.isoformat(),
            'range_end_date':   self.re.isoformat(),
            'search_string':    self.name,
        })])

    def _expand(self):
        """600+ filtered results — drill deeper."""
        if len(self.name) <= 6:
            children = [
                json.dumps({
                    'range_start_date': self.rs.isoformat(),
                    'range_end_date':   self.re.isoformat(),
                    'search_string':    self.name + c,
                })
                for c in FIRST_CHARS
            ]
            push_items(children)
            logger.info(f'Expanded {self.id} → {len(children)} children')
        elif self.rs < self.re:
            mid = self.rs + (self.re - self.rs) // 2
            push_items([
                json.dumps({'range_start_date': self.rs.isoformat(),
                            'range_end_date': mid.isoformat(),
                            'search_string': self.name}),
                json.dumps({'range_start_date': (mid + timedelta(1)).isoformat(),
                            'range_end_date': self.re.isoformat(),
                            'search_string': self.name}),
            ])
            logger.info(f'Split {self.id} into 2 date ranges')


# ── Spider (trio + thread pool) ───────────────────────────────────────────────
class FCSpider:
    def __init__(self, concurrency=CONCURRENCY):
        self.concurrency = concurrency
        self.solver = DataDomeSolver()

    def run(self, forever=False):
        trio.run(self._start, forever)

    async def _start(self, forever):
        logger.info(f'FC spider v2 starting (concurrency={self.concurrency})')

        # Pre-solve DataDome before starting workers
        logger.info('Pre-solving DataDome...')
        await trio.to_thread.run_sync(self.solver.solve)

        semaphore = trio.Semaphore(self.concurrency)

        try:
            async with trio.open_nursery() as nursery:
                while True:
                    items = pop_items(min(5, self.concurrency))
                    if items:
                        for raw in items:
                            b = json.loads(raw)
                            node = SearchNode(
                                self.solver,
                                datetime.fromisoformat(b['range_start_date']),
                                datetime.fromisoformat(b['range_end_date']),
                                b['search_string'],
                            )
                            await semaphore.acquire()
                            nursery.start_soon(self._run_node, node, semaphore)
                    else:
                        remaining = qlen()
                        logger.info(f'Queue empty (remaining={remaining})')
                        if not forever:
                            break
                        await trio.sleep(300)
        except KeyboardInterrupt:
            logger.info('Stopped.')

    async def _run_node(self, node, semaphore):
        try:
            await trio.to_thread.run_sync(node.search, abandon_on_cancel=True)
        finally:
            semaphore.release()


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Foreclosure Spider v2 (Playwright + curl_cffi)')
    p.add_argument('--seed',        action='store_true')
    p.add_argument('--run',         action='store_true')
    p.add_argument('--forever',     action='store_true')
    p.add_argument('--concurrency', type=int, default=CONCURRENCY)
    p.add_argument('--queue-len',   action='store_true')
    p.add_argument('--clear-done',  action='store_true')
    p.add_argument('--clear-queue', action='store_true')
    p.add_argument('--start-date',  type=str, default='2024-01-01')
    p.add_argument('--end-date',    type=str, default=None)
    args = p.parse_args()

    if args.queue_len:
        print(f'Queue: {qlen():,}')
        done_count = get_redis().scard(DONE_SET_KEY)
        print(f'Completed slices: {done_count:,}')
        sys.exit(0)

    if args.clear_done:
        clear_done()
        logger.info('Cleared completed slices')

    if args.clear_queue:
        get_redis().delete(QUEUE_KEY)
        logger.info('Cleared queue')

    start = datetime.strptime(args.start_date, '%Y-%m-%d')
    end = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else datetime.now()

    if args.seed:
        seed_queue(start, end)

    if args.run:
        FCSpider(concurrency=args.concurrency).run(forever=args.forever)
