import logging
import random
import string
from collections import OrderedDict

import httpcore
import httpx
import trio
from bs4 import BeautifulSoup

from .config import config

logger = logging.getLogger('mjcs')


class Forbidden(Exception):
    pass

class DataDomeBlocked(Exception):
    pass


class AsyncSessionPool:
    def __init__(self, concurrency):
        self.concurrency = concurrency
        self.send_channel, self.receive_channel = trio.open_memory_channel(max_buffer_size=self.concurrency)
        for _ in range(self.concurrency):
            self.send_channel.send_nowait(AsyncSession())

    async def get(self):
        return await self.receive_channel.receive()

    async def put(self, session):
        await self.send_channel.send(session)

    def put_nowait(self, session):
        self.send_channel.send_nowait(session)


class AsyncSession:
    def __init__(self):
        self.new_session()

    def _build_proxy_url(self, session_name):
        """Build proxy URL supporting BrightData/Smartproxy/IPRoyal (preferred)
        or ScraperAPI (fallback via SCRAPERAPI_KEY env var)."""
        import os
        proxy_username = os.getenv('PROXY_USERNAME') or getattr(config, 'PROXY_USERNAME', None)
        proxy_password = os.getenv('PROXY_PASSWORD') or getattr(config, 'PROXY_PASSWORD', None)
        proxy_host     = os.getenv('PROXY_HOST')     or getattr(config, 'PROXY_HOST', None)
        proxy_port     = os.getenv('PROXY_PORT')     or getattr(config, 'PROXY_PORT', None)
        scraperapi_key = os.getenv('SCRAPERAPI_KEY') or getattr(config, 'SCRAPERAPI_KEY', None)

        if proxy_username and proxy_password and proxy_host and proxy_port:
            # Plain format: works with Webshare (-rotate), BrightData, Smartproxy, IPRoyal
            url = f'http://{proxy_username}:{proxy_password}@{proxy_host}:{proxy_port}'
            logger.info(f'Using proxy via {proxy_host}:{proxy_port}')
            return url

        if scraperapi_key:
            # ScraperAPI sticky-session fallback (session_number must be numeric)
            sid = ''.join(random.choices(string.digits, k=5))
            url = f'http://scraperapi.session_number={sid}:{scraperapi_key}@proxy-server.scraperapi.com:8001'
            logger.info(f'Using ScraperAPI sticky session {sid}')
            return url

        logger.warning('No proxy configured — connecting directly (may be blocked by MJCS)')
        return None

    def new_session(self):
        headers = OrderedDict({
            'Sec-Ch-Device-Memory': '8',
            'Sec-Ch-Ua': '"Microsoft Edge";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Arch': '"x86"',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Ch-Ua-Model': '""',
            'Sec-Ch-Ua-Full-Version-List': '"Microsoft Edge";v="125.0.2535.92", "Chromium";v="125.0.6422.142", "Not.A/Brand";v="24.0.0.0"',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Priority': 'u=0, i',
        })
        session_name = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=10))
        proxy = self._build_proxy_url(session_name)
        self.session = httpx.AsyncClient(
            headers=headers,
            proxy=proxy,
            verify=False,
            follow_redirects=True
        )
        # Inject DataDome cookie (fetched via Playwright) to bypass bot protection
        import json as _json, os as _os
        _cookie_file = '/tmp/datadome_cookie.json'
        if _os.path.exists(_cookie_file):
            try:
                _dd = _json.load(open(_cookie_file)).get('datadome', '')
                if _dd:
                    self.session.cookies.set('datadome', _dd, domain='casesearch.courts.state.md.us')
                    logger.debug('Injected DataDome cookie from file')
            except Exception:
                pass

    async def request(self, *args, i=1, **kwargs):
        if i > 12:
            raise Exception('Too many retried requests')
        # Random delay to avoid rate limiting / DataDome detection
        await trio.sleep(random.uniform(4, 7))
        try:
            response = await self.session.request(
                *args,
                **kwargs,
                timeout=config.QUERY_TIMEOUT
            )

            # Detect DataDome bot protection challenge
            if ('datadome' in response.text.lower() or
                    'captcha-delivery.com' in response.text or
                    'Please enable JS and disable any ad blocker' in response.text):
                logger.warning(f'DataDome challenge detected — rotating session (attempt {i})')
                await self.session.aclose()
                await trio.sleep(i * 10)
                self.new_session()
                if i >= 5:
                    raise DataDomeBlocked('DataDome is blocking this proxy IP')
                await self.renew()
                return await self.request(*args, i=i + 1, **kwargs)

            if ((response.history and response.history[0].status_code == 302 and
                        response.history[0].headers.get('location', '') == f'{config.MJCS_BASE_URL}/inquiry-index.jsp')
                    or "Acceptance of the following agreement is" in response.text):
                logger.debug('Disclaimer detected — renewing session...')
                await self.renew()
                return await self.request(*args, i=i + 1, **kwargs)
            elif response.status_code == 403:
                logger.debug('403 Forbidden — rotating session...')
                await self.session.aclose()
                await trio.sleep(i * 5)
                self.new_session()
                await self.renew()
                return await self.request(*args, i=i + 1, **kwargs)
            return response
        except (httpx.TransportError, httpcore.TimeoutException) as e:
            logger.debug(f'{type(e).__name__} error, retrying...')
            await trio.sleep(i * 5)
            self.new_session()
            return await self.request(*args, i=i + 1, **kwargs)

    async def renew(self, i=1):
        if i > 12:
            raise Exception('Too many retried renewals')
        try:
            response = await self.session.request(
                'GET',
                f'{config.MJCS_BASE_URL}/'
            )
        except Exception as e:
            logger.debug(f'renew GET error: {e}')
            await self.session.aclose()
            await trio.sleep(i * 5)
            self.new_session()
            return await self.renew(i=i + 1)

        soup = BeautifulSoup(response.text, 'html.parser')
        disclaimer = soup.find('input', {'name': 'disclaimer'})
        if not disclaimer:
            logger.debug('renew: no disclaimer form found — session may already be valid')
            return

        disclaimer_token = disclaimer.get('value')
        self.session.headers.update({
            'Cache-Control': 'max-age=0',
            'Origin': config.MJCS_SITE,
            'Sec-Fetch-Site': 'same-origin',
        })
        try:
            response = await self.session.request(
                'POST',
                f'{config.MJCS_BASE_URL}/processDisclaimer.jis',
                data={'disclaimer': disclaimer_token},
                headers={'Referer': f'{config.MJCS_BASE_URL}/'}
            )
        except Exception as e:
            logger.debug(f'renew POST error: {e}')
            await self.session.aclose()
            await trio.sleep(i * 5)
            self.new_session()
            return await self.renew(i=i + 1)

        # Success: MJCS redirects to inquiry-search.jsp — checking body only (not history)
        soup2 = BeautifulSoup(response.text, 'html.parser')
        if response.status_code != 200 or soup2.find('input', {'name': 'disclaimer'}):
            logger.debug(f'renew failed: code={response.status_code} — rotating session')
            await self.session.aclose()
            await trio.sleep(i * 5)
            self.new_session()
            return await self.renew(i=i + 1)

        logger.info('Session renewed successfully')
        # Warm up DataDome session by visiting the search page before making queries
        try:
            await trio.sleep(random.uniform(2, 4))
            await self.session.request(
                'GET',
                f'{config.MJCS_BASE_URL}/inquiry-search.jsp',
                headers={'Referer': f'{config.MJCS_BASE_URL}/inquiry-index.jsp'}
            )
            logger.debug('DataDome warmup: visited inquiry-search.jsp')
        except Exception:
            pass

# ─────────────────────────────────────────────────────────
# Playwright-based sessions (bypasses DataDome via real browser)
# Runs in background threads with their own asyncio event loops.
# Communicates with the trio spider via thread-safe queues.
# ─────────────────────────────────────────────────────────
import asyncio
import os
import queue as thread_queue
import threading


class PlaywrightResponse:
    """Mimics httpx.Response so spider.py needs zero changes."""
    def __init__(self, status, text, headers, history=None):
        self.status_code = status
        self.text        = text
        self.headers     = headers
        self.history     = history or []


class PlaywrightWorker:
    """
    Runs a Playwright browser in a daemon thread with its own asyncio loop.
    Keeps the browser alive across many requests (DataDome solved once).
    Thread-safe: spider sends requests via queue, gets responses back.
    """

    MJCS_HOME = 'https://casesearch.courts.state.md.us/casesearch/inquiry-index.jsp'

    def __init__(self, session_id: str):
        self.session_id   = session_id
        self._req_queue   = thread_queue.Queue()
        self._ready_event = threading.Event()
        self._error       = None
        self._thread      = threading.Thread(target=self._thread_main, daemon=True, name=f'pw-{session_id}')
        self._thread.start()
        # Wait until browser is ready (or errored)
        self._ready_event.wait(timeout=60)
        if self._error:
            raise RuntimeError(f'[{session_id}] Playwright startup failed: {self._error}')

    def _proxy_cfg(self):
        user = os.getenv('PROXY_USERNAME', 'oehwhsaf-rotate')
        pw   = os.getenv('PROXY_PASSWORD', 'w03c3ohqnnbp')
        host = os.getenv('PROXY_HOST',     'p.webshare.io')
        port = os.getenv('PROXY_PORT',     '80')
        # Use sticky session per worker so IP stays consistent
        return {
            'server':   f'http://{host}:{port}',
            'username': user,
            'password': pw,
        }

    def _thread_main(self):
        try:
            asyncio.run(self._async_main())
        except Exception as e:
            self._error = str(e)
            self._ready_event.set()

    async def _async_main(self):
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                proxy=self._proxy_cfg(),
                args=['--no-sandbox', '--disable-setuid-sandbox',
                      '--disable-dev-shm-usage',
                      '--disable-blink-features=AutomationControlled'],
            )
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

            # Warm up: visit MJCS to solve DataDome + disclaimer
            await self._renew(context)
            self._ready_event.set()
            logger.info(f'[{self.session_id}] Playwright worker ready')

            # Main request loop
            while True:
                try:
                    item = self._req_queue.get(timeout=0.2)
                except thread_queue.Empty:
                    continue
                if item is None:
                    break  # shutdown signal
                method, url, data, headers, result_q = item
                try:
                    result = await self._do_request(context, method, url, data, headers)
                    result_q.put(result)
                except Exception as e:
                    result_q.put({'error': str(e)})

            await browser.close()

    async def _renew(self, context, attempt=1, apply_stealth=False):
        """Navigate to MJCS in a real browser page — solves DataDome + disclaimer."""
        if attempt > 5:
            raise RuntimeError(f'[{self.session_id}] Could not pass DataDome after 5 attempts')
        page = await context.new_page()
        if apply_stealth:
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except Exception:
                pass
        try:
            await page.goto(self.MJCS_HOME, wait_until='networkidle', timeout=30000)
            await asyncio.sleep(4)
            content = await page.content()

            if 'Access is temporarily restricted' in content or \
               ('datadome' in content.lower() and 'captcha' in content.lower()):
                logger.warning(f'[{self.session_id}] DataDome block on renew attempt {attempt}')
                await page.close()
                await asyncio.sleep(15)
                return await self._renew(context, attempt + 1)

            # Accept disclaimer if present
            try:
                disc = page.locator('input[name="disclaimer"]')
                if await disc.count() > 0:
                    await disc.click()
                    await page.wait_for_load_state('networkidle')
                    await asyncio.sleep(2)
            except Exception:
                pass
        finally:
            await page.close()

    async def _do_request(self, context, method, url, data, headers):
        resp = await context.request.fetch(
            url,
            method=method,
            form=data,
            headers=headers or {},
            timeout=135000,
        )
        text = await resp.text()
        # If DataDome challenge appears on API response, re-solve and retry once
        if 'Access is temporarily restricted' in text or \
           ('datadome' in text.lower() and 'captcha' in text.lower()):
            logger.warning(f'[{self.session_id}] DataDome on API call — re-solving...')
            await self._renew(context)
            resp = await context.request.fetch(
                url, method=method, form=data, headers=headers or {}, timeout=135000
            )
            text = await resp.text()
        return {'status': resp.status, 'text': text, 'headers': dict(resp.headers)}

    def sync_request(self, method, url, data=None, headers=None, timeout=180):
        """Blocking call — safe to call from any thread."""
        result_q = thread_queue.Queue()
        self._req_queue.put((method, url, data, headers, result_q))
        result = result_q.get(timeout=timeout)
        if 'error' in result:
            raise Exception(result['error'])
        return result

    def shutdown(self):
        self._req_queue.put(None)
        self._thread.join(timeout=10)


class PlaywrightSession:
    """Async wrapper around PlaywrightWorker for use in trio via to_thread."""

    def __init__(self, session_id: str):
        self.session_id = session_id
        self._worker    = None  # initialized in start()

    def start_sync(self):
        """Called from a thread to initialize the worker."""
        self._worker = PlaywrightWorker(self.session_id)

    async def request(self, method='GET', url=None, data=None,
                      headers=None, timeout=None, **kwargs):
        import trio
        result = await trio.to_thread.run_sync(
            lambda: self._worker.sync_request(method, url, data, headers, timeout or 180),
            abandon_on_cancel=True,
        )
        return PlaywrightResponse(result['status'], result['text'], result['headers'])

    async def renew(self):
        pass  # Playwright handles sessions automatically

    async def aclose(self):
        if self._worker:
            await trio.to_thread.run_sync(self._worker.shutdown, abandon_on_cancel=True)


class PlaywrightSessionPool:
    """Drop-in replacement for AsyncSessionPool using Playwright browsers."""

    def __init__(self, concurrency: int):
        self.concurrency  = concurrency
        self._sessions    = []
        self._send_ch     = None
        self._recv_ch     = None

    async def start_all(self):
        """Initialize all browser sessions (each in its own thread)."""
        import trio
        self._send_ch, self._recv_ch = trio.open_memory_channel(
            max_buffer_size=self.concurrency
        )
        for i in range(self.concurrency):
            sid = f'SP{i:02d}'
            s = PlaywrightSession(sid)
            # Start the browser in a thread (blocks until browser is ready)
            await trio.to_thread.run_sync(s.start_sync, abandon_on_cancel=True)
            self._sessions.append(s)
            self._send_ch.send_nowait(s)
            if i < self.concurrency - 1:
                await trio.sleep(15)  # stagger browser startups

    async def get(self):
        return await self._recv_ch.receive()

    def put_nowait(self, session):
        self._send_ch.send_nowait(session)

    async def close_all(self):
        import trio
        for s in self._sessions:
            await s.aclose()
