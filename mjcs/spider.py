"""
Spider rewritten for new React SPA API (Case Portal 1.0).
Uses curl_cffi firefox135 to bypass DataDome.
POST /api-caselist/v1/cases  →  JSON array of up to 600 cases.
Includes delays between requests to avoid rate-limiting.
"""
import json
import os
import logging
import random
import string
import time
from datetime import datetime, timedelta

import trio
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from .config import config
from .models import Case
from .util import db_session, send_to_queue, split_date_range

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

logger = logging.getLogger('mjcs')

BASE_URL = 'https://casesearch.courts.state.md.us'
SEARCH_URL = f'{BASE_URL}/api-caselist/v1/cases'
MAX_RESULTS = 600  # API hard cap

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

# Characters for 2-char prefix generation
FIRST_CHARS = string.ascii_uppercase + string.digits
SECOND_CHARS = string.ascii_uppercase + string.digits + ' '

class FailedSearch(Exception):
    pass

class CompletedSearchNoResults(Exception):
    pass


def generate_spider_slices(range_start_date, range_end_date=None, court=None, site=None):
    """
    Generate 2-char last name prefix × date range slices and push to queue.
    With 16 days/query and ~23 date ranges per year, and ~1332 prefixes,
    this yields ~30k initial slices.
    """
    if range_end_date is None:
        range_end_date = datetime.now()

    days = config.SPIDER_DAYS_PER_QUERY  # default 16

    slices = []
    current = range_start_date
    while current <= range_end_date:
        end = min(current + timedelta(days - 1), range_end_date)
        for c1 in FIRST_CHARS:
            for c2 in SECOND_CHARS:
                slices.append(json.dumps({
                    'range_start_date': current.isoformat(),
                    'range_end_date': end.isoformat(),
                    'search_string': c1 + c2,
                    'court': court,
                }))
        current = end + timedelta(1)

    logger.info(f'Submitting {len(slices)} slices for spidering')
    send_to_queue(config.spider_queue, slices)


_PROFILES = ['firefox133', 'firefox135', 'firefox144']
_REFRESH_AFTER = 25  # replace session with a fresh one every N uses

class CurlSession:
    """Pool of curl_cffi sessions with auto-refresh to avoid DataDome rate limits."""

    def __init__(self, size):
        self._size = size
        self._send_chan = None
        self._recv_chan = None
        self._use_counts = {}
        self._profile_idx = 0
        _puser = os.getenv('PROXY_USERNAME', '')
        _ppass = os.getenv('PROXY_PASSWORD', '')
        _phost = os.getenv('PROXY_HOST', '')
        _pport = os.getenv('PROXY_PORT', '80')
        self._proxies = {'https': f'http://{_puser}:{_ppass}@{_phost}:{_pport}'} if _phost else {}

    def _new_session(self):
        profile = _PROFILES[self._profile_idx % len(_PROFILES)]
        self._profile_idx += 1
        s = cffi_requests.Session(impersonate=profile, proxies=self._proxies)
        self._use_counts[id(s)] = 0
        return s

    async def start_all(self):
        self._send_chan, self._recv_chan = trio.open_memory_channel(self._size)
        for _ in range(self._size):
            await self._send_chan.send(self._new_session())

    async def get(self):
        return await self._recv_chan.receive()

    def put_nowait(self, session):
        self._use_counts[id(session)] = self._use_counts.get(id(session), 0) + 1
        if self._use_counts[id(session)] >= _REFRESH_AFTER:
            session = self._new_session()
        try:
            self._send_chan.send_nowait(session)
        except trio.WouldBlock:
            pass

    async def close_all(self):
        pass  # curl_cffi sessions close automatically


class Spider:
    def __init__(self, concurrency=3):
        self.concurrency = concurrency
        self.session_pool = CurlSession(concurrency)

    def spider_from_queue(self, forever=False):
        trio.run(self.__start_service, forever)

    async def __start_service(self, forever):
        logger.info('Initiating spider service.')
        await self.session_pool.start_all()
        logger.info(f'Started {self.concurrency} curl_cffi sessions.')
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(self.__queue_manager, nursery, forever)
        except KeyboardInterrupt:
            print('Caught KeyboardInterrupt: stopping service.')
        await self.session_pool.close_all()
        logger.info('Spider service stopped.')

    async def __queue_manager(self, nursery, forever):
        while True:
            if len(nursery.child_tasks) < 100:
                queue_items = config.spider_queue.receive_messages(
                    WaitTimeSeconds=config.QUEUE_WAIT,
                    MaxNumberOfMessages=10,
                )
                if queue_items:
                    for item in queue_items:
                        body = json.loads(item.body)
                        node = SearchNode(
                            self.session_pool,
                            datetime.fromisoformat(body['range_start_date']),
                            datetime.fromisoformat(body['range_end_date']),
                            body['search_string'],
                            body.get('court'),
                        )
                        nursery.start_soon(node.search)
                        item.delete()
                else:
                    logger.info('No items in spider queue.')
                    if not forever:
                        break
                    await trio.sleep(5 * 60)
            else:
                await trio.sleep(5)


class SearchNode:
    def __init__(self, session_pool, range_start_date, range_end_date, search_string, court=None):
        self.session_pool = session_pool
        self.range_start_date = range_start_date
        self.range_end_date = range_end_date
        self.search_string = search_string
        self.court = court

    @property
    def id(self):
        d = f'{self.range_start_date:%Y-%m-%d}/{self.range_end_date:%Y-%m-%d}'
        return f'{d}/{self.search_string}'

    async def search(self):
        session = await self.session_pool.get()
        try:
            # Run blocking HTTP call in a thread
            results = await trio.to_thread.run_sync(
                lambda: self._do_search(session)
            )
        finally:
            self.session_pool.put_nowait(session)

        if results is None:
            return

        if len(results) >= MAX_RESULTS:
            if len(self.search_string) <= 15:
                self._spawn_children()
            elif self.range_start_date != self.range_end_date:
                self._split()

    def _do_search(self, session):
        """Synchronous search — runs in a thread."""
        body = {
            'searchPartyType': 'Person',
            'lastName': self.search_string,
            'firstName': '',
            'middleName': '',
            'businessName': '',
            'startDate': self.range_start_date.strftime('%-m/%-d/%Y'),
            'endDate': self.range_end_date.strftime('%-m/%-d/%Y'),
        }
        if self.court:
            body['county'] = self.court

        # Polite delay: 2–5 s jitter between requests
        time.sleep(random.uniform(2.0, 5.0))

        try:
            r = session.post(
                SEARCH_URL,
                headers=SEARCH_HEADERS,
                data=json.dumps(body),
                timeout=90,
            )
        except Exception as e:
            logger.warning(f'Request error for {self.id}: {e}')
            # Re-queue for later retry
            send_to_queue(config.spider_queue, [json.dumps({
                'range_start_date': self.range_start_date.isoformat(),
                'range_end_date': self.range_end_date.isoformat(),
                'search_string': self.search_string,
                'court': self.court,
            })])
            return None

        if r.status_code == 403:
            # Drop old-format slices with punctuation — they will never succeed
            # Only alphanumeric + space are valid search prefixes for the new API
            valid_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ')
            if not all(c in valid_chars for c in self.search_string.upper()):
                logger.debug(f'Dropping invalid search string {self.search_string!r}')
                return []
            if 'captcha-delivery' in r.text:
                logger.warning(f'DataDome blocked {self.id} — re-queuing')
            else:
                logger.warning(f'403 for {self.id}: {r.text[:200]}')
            send_to_queue(config.spider_queue, [json.dumps({
                'range_start_date': self.range_start_date.isoformat(),
                'range_end_date': self.range_end_date.isoformat(),
                'search_string': self.search_string,
                'court': self.court,
            })])
            return None

        if r.status_code == 400:
            # "CaseSearch will only display results..." — empty result set
            return []

        if r.status_code != 200:
            logger.warning(f'{r.status_code} for {self.id}: {r.text[:200]}')
            return None

        try:
            data = json.loads(r.text)
        except Exception:
            logger.warning(f'JSON parse error for {self.id}: {r.text[:200]}')
            return None

        if not isinstance(data, list):
            logger.warning(f'Unexpected response for {self.id}: {type(data)} {str(data)[:200]}')
            return None

        # Deduplicate and build case records
        seen = {}
        for row in data:
            cid = row.get('caseNumber', '').strip()
            if not cid or cid in seen:
                continue
            fd_str = row.get('filingDate', '') or ''
            filing_date = None
            if fd_str:
                try:
                    filing_date = datetime.strptime(fd_str, '%m/%d/%Y').date()
                except ValueError:
                    pass
            seen[cid] = {
                'case_number': cid,
                'court': row.get('locationName', ''),
                'case_type': row.get('caseType', ''),
                'status': row.get('caseStatus', ''),
                'filing_date': filing_date,
                'filing_date_original': fd_str,
                'caption': row.get('title', ''),
                'detail_loc': 'MJCS2',
            }

        if not seen:
            logger.debug(f'{self.id}: 0 results')
            return []

        with db_session() as db:
            existing = set(db.scalars(
                select(Case.case_number)
                .where(Case.case_number.in_(seen.keys()))
            ).all())
            new_cases = [v for k, v in seen.items() if k not in existing]

            if new_cases:
                db.execute(insert(Case).values(new_cases).on_conflict_do_nothing())
                send_to_queue(config.scraper_queue, [
                    json.dumps({'case_number': c['case_number'], 'detail_loc': c['detail_loc']})
                    for c in new_cases
                ])
                logger.info(f'{self.id}: +{len(new_cases)} new / {len(data)} total')
            else:
                logger.debug(f'{self.id}: {len(data)} results, all known')

        return data

    def _spawn_children(self):
        slices = [
            json.dumps({
                'range_start_date': self.range_start_date.isoformat(),
                'range_end_date': self.range_end_date.isoformat(),
                'search_string': self.search_string + c,
                'court': self.court,
            })
            for c in (FIRST_CHARS + ' ')
        ]
        send_to_queue(config.spider_queue, slices)
        logger.info(f'Spawned {len(slices)} children for {self.id}')

    def _split(self):
        r1, r2 = split_date_range(self.range_start_date, self.range_end_date)
        send_to_queue(config.spider_queue, [
            json.dumps({
                'range_start_date': r1[0].isoformat(),
                'range_end_date': r1[1].isoformat(),
                'search_string': self.search_string,
                'court': self.court,
            }),
            json.dumps({
                'range_start_date': r2[0].isoformat(),
                'range_end_date': r2[1].isoformat(),
                'search_string': self.search_string,
                'court': self.court,
            }),
        ])
        logger.debug(f'Split date range for {self.id}')
