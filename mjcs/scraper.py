"""
Scraper rewritten for new React SPA API (Case Portal 1.0).
Uses curl_cffi firefox135 to bypass DataDome.
GET /api-casedetails/v1/public/cases/{caseId}  →  JSON case detail.
NOTE: Must NOT include 'Origin' header — triggers CORS rejection.
Uses fresh session per case + delays to stay under DataDome's radar.
"""
import json
import os
import logging
import random
import time
from datetime import datetime
from hashlib import sha256

import botocore
import trio
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from .config import config
from .models import Case, Scrape, ScrapeVersion
from .util import db_session, send_to_queue

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

logger = logging.getLogger('mjcs')

BASE_URL = 'https://casesearch.courts.state.md.us'
DETAIL_URL = f'{BASE_URL}/api-casedetails/v1/public/cases'
WARMUP_URL = f'{BASE_URL}/casesearch'  # pre-warm session to get DataDome cookies

# Headers for the warm-up page visit (gets DataDome cookie)
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

# No Origin header — CORS check rejects it when Origin is set
DETAIL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://casesearch.courts.state.md.us/casesearch',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

# Rotate through Firefox profiles to vary TLS fingerprints
PROFILES = ['firefox135', 'firefox133', 'firefox135', 'firefox133', 'firefox135']
_profile_idx = 0

def _next_profile():
    global _profile_idx
    p = PROFILES[_profile_idx % len(PROFILES)]
    _profile_idx += 1
    return p


class FailedScrape(Exception):
    pass

class FailedScrapeNotFound(FailedScrape):
    pass

class FailedScrapeUnknownError(FailedScrape):
    pass

# Compatibility shim — harvester.py imports Forbidden from scraper
class Forbidden(Exception):
    pass


class Scraper:
    def __init__(self, concurrency=2):
        self.concurrency = concurrency

    def scrape_from_queue(self, forever=False):
        trio.run(self.__start_service, forever)

    def scrape_case(self, case_number):
        async def _run():
            await trio.to_thread.run_sync(
                lambda: self._scrape_case_sync(case_number)
            )
        trio.run(_run)

    # rescrape_stale and count_stale kept for compatibility with harvester.py
    def stale_filter(self, range_start_date=None, range_end_date=None,
                     include_unscraped=False, include_inactive=False):
        from sqlalchemy import and_, or_, text
        from .models import Case
        or_filters = [and_(
            Case.active == True,
            or_(
                text('cases.filing_date > current_date'),
                or_(
                    text(f'age_days(cases.last_scrape) > ceiling({config.RESCRAPE_COEFFICIENT}*age_days(cases.filing_date))'),
                    text(f'age_days(cases.last_scrape) > {config.MAX_SCRAPE_AGE}')
                )
            )
        )]
        if include_unscraped:
            or_filters.append(Case.last_scrape == None)
        if include_inactive:
            from sqlalchemy import and_, or_, text
            or_filters.append(and_(
                Case.active == False,
                or_(
                    text('cases.filing_date > current_date'),
                    text(f'age_days(cases.last_scrape) > {config.MAX_SCRAPE_AGE_INACTIVE}')
                )
            ))
        filters = [Case.scrape_exempt == False, or_(*or_filters)]
        if range_start_date:
            filters.append(Case.filing_date >= range_start_date)
        if range_end_date:
            filters.append(Case.filing_date <= range_end_date)
        return and_(*filters)

    def count_stale(self, range_start_date=None, range_end_date=None,
                    include_unscraped=False, include_inactive=False):
        with db_session() as db:
            return db.scalar(
                select(func.count(Case.case_number))
                .where(self.stale_filter(range_start_date, range_end_date,
                                         include_unscraped, include_inactive))
            )

    def rescrape_stale(self, range_start_date=None, range_end_date=None,
                       include_unscraped=False, include_inactive=False):
        from .util import get_queue_count
        if get_queue_count(config.scraper_queue) > config.SCRAPE_QUEUE_THRESHOLD:
            logger.info('Scraper queue already full, aborting...')
            return
        total = 0
        with db_session() as db:
            partitions = db.execute(
                select(Case.case_number, Case.detail_loc)
                .where(self.stale_filter(range_start_date, range_end_date,
                                         include_unscraped, include_inactive))
                .execution_options(yield_per=10)
            ).partitions()
            for partition in partitions:
                messages = [
                    json.dumps({'case_number': c[0], 'detail_loc': c[1]})
                    for c in partition
                ]
                send_to_queue(config.scraper_queue, messages)
                total += len(partition)
        logger.info(f'Submitted {total} stale cases for rescraping')

    async def __start_service(self, forever):
        logger.info('Initiating scraper service.')
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(self.__queue_manager, nursery, forever)
        except KeyboardInterrupt:
            print('\nCaught KeyboardInterrupt: stopping service.')
        logger.info('Scraper service stopped.')

    async def __queue_manager(self, nursery, forever):
        # Use a semaphore to limit concurrency
        semaphore = trio.Semaphore(self.concurrency)
        while True:
            if len(nursery.child_tasks) < self.concurrency * 10:
                queue_items = config.scraper_queue.receive_messages(
                    WaitTimeSeconds=config.QUEUE_WAIT,
                    MaxNumberOfMessages=self.concurrency,
                )
                if queue_items:
                    for item in queue_items:
                        body = json.loads(item.body)
                        case_number = body['case_number']
                        nursery.start_soon(
                            self.__scrape_task, case_number, semaphore
                        )
                        item.delete()
                else:
                    logger.debug('No items in scraper queue.')
                    if not forever:
                        break
                    await trio.sleep(5 * 60)
            else:
                await trio.sleep(5)

    async def __scrape_task(self, case_number, semaphore):
        async with semaphore:
            await trio.to_thread.run_sync(
                lambda: self._scrape_case_sync(case_number)
            )

    def _scrape_case_sync(self, case_number):
        """Synchronous scrape — fresh session per case, with delay."""
        # Skip if already scraped — avoids wasting HTTP requests on duplicates in queue
        try:
            from .models import Case
            from .util import db_session
            with db_session() as session:
                row = session.query(Case).filter_by(case_number=case_number).first()
                if row and row.last_scrape is not None:
                    logger.debug(f'{case_number}: already scraped, skipping')
                    return
        except Exception:
            pass  # If DB check fails, proceed with scrape anyway

        # Random delay between cases to stay under rate limits
        time.sleep(random.uniform(0.5, 1.5))


        _puser = os.getenv('PROXY_USERNAME', '')
        _ppass = os.getenv('PROXY_PASSWORD', '')
        _phost = os.getenv('PROXY_HOST', '')
        _pport = os.getenv('PROXY_PORT', '80')
        proxies = {'https': f'http://{_puser}:{_ppass}@{_phost}:{_pport}'} if _phost else {}
        session = cffi_requests.Session(impersonate=_next_profile(), proxies=proxies)

        # Pre-warm: visit the main search page to get DataDome cookies
        try:
            session.get(WARMUP_URL, headers=WARMUP_HEADERS, timeout=(10, 20))
            time.sleep(random.uniform(0.3, 0.8))  # brief pause like a real user
        except Exception:
            pass  # warm-up failure is non-fatal, try API anyway

        begin = datetime.now()
        try:
            r = session.get(
                f'{DETAIL_URL}/{case_number}',
                headers=DETAIL_HEADERS,
                timeout=(10, 30),  # 10s connect timeout, 30s read timeout
            )
        except Exception as e:
            logger.warning(f'Request error for {case_number}: {e}')
            send_to_queue(config.scraper_queue, [
                json.dumps({'case_number': case_number, 'detail_loc': 'MJCS2'})
            ])
            return

        end = datetime.now()
        duration = (end - begin).total_seconds()

        if r.status_code == 403 and 'captcha-delivery' in r.text:
            logger.warning(f'{case_number}: DataDome 403 — re-queuing')
            # Longer back-off before retry
            time.sleep(random.uniform(3.0, 6.0))
            send_to_queue(config.scraper_queue, [
                json.dumps({'case_number': case_number, 'detail_loc': 'MJCS2'})
            ])
            return
        elif r.status_code == 404:
            logger.warning(f'{case_number}: 404 Not Found')
            self._record_error(case_number, 'FailedScrapeNotFound', begin)
            return
        elif r.status_code != 200:
            logger.warning(f'{case_number}: HTTP {r.status_code}')
            self._record_error(case_number, f'HTTP{r.status_code}', begin)
            return

        try:
            json.loads(r.text)  # validate JSON
        except Exception:
            logger.warning(f'{case_number}: JSON parse error')
            self._record_error(case_number, 'JSONParseError', begin)
            return

        self._store(case_number, r.text, begin, duration)

    def _record_error(self, case_number, error_name, timestamp):
        with db_session() as db:
            db.add(Scrape(
                case_number=case_number,
                timestamp=timestamp,
                error=error_name,
            ))
            err_count = db.scalar(
                select(func.count())
                .select_from(Scrape)
                .where(Scrape.case_number == case_number)
                .where(Scrape.error != None)
            )
            if err_count >= 3:
                db.execute(
                    Case.__table__.update()
                    .where(Case.case_number == case_number)
                    .values(scrape_exempt=True)
                )

    def _store(self, case_number, body_text, timestamp, duration):
        new_sha = sha256(body_text.encode('utf-8')).hexdigest()

        with db_session() as db:
            latest_sha = db.scalars(
                select(ScrapeVersion.sha256)
                .join(Scrape, ScrapeVersion.s3_version_id == Scrape.s3_version_id)
                .where(Scrape.case_number == case_number)
                .order_by(Scrape.timestamp.desc())
            ).first()

        if latest_sha == new_sha:
            with db_session() as db:
                db.execute(
                    Case.__table__.update()
                    .where(Case.case_number == case_number)
                    .values(last_scrape=timestamp)
                )
            logger.debug(f'{case_number}: unchanged')
            return

        if not latest_sha:
            logger.info(f'{case_number}: new scrape')
        else:
            logger.info(f'{case_number}: updated version')

        obj = config.case_details_bucket.put_object(
            Body=body_text,
            Key=case_number,
            Metadata={
                'timestamp': timestamp.isoformat(),
                'detail_loc': 'MJCS2',
            },
        )
        try:
            version_id = obj.version_id
        except botocore.exceptions.ClientError:
            time.sleep(5)
            version_id = obj.version_id

        try:
            with db_session() as db:
                sv = ScrapeVersion(
                    s3_version_id=version_id,
                    case_number=case_number,
                    length=len(body_text),
                    sha256=new_sha,
                )
                scrape = Scrape(
                    case_number=case_number,
                    s3_version_id=version_id,
                    timestamp=timestamp,
                    duration=duration,
                )
                db.add(sv)
                db.flush()
                db.add(scrape)
        except IntegrityError:
            logger.info(f'{case_number}: already in scrape_versions, skipping duplicate insert')

        with db_session() as db:
            db.execute(
                Case.__table__.update()
                .where(Case.case_number == case_number)
                .values(last_scrape=timestamp)
            )
