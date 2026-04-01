"""
Playwright-based sessions with comprehensive stealth to bypass DataDome.
Uses Webshare residential rotating proxies.
Runs in background threads (own asyncio loop each) — compatible with trio spider.
"""
import asyncio
import os
import queue as thread_queue
import threading
import logging
import random

logger = logging.getLogger(__name__)

# ── Comprehensive stealth JS ──────────────────────────────────────────────────
# Applied at context level so it fires on every page/frame before any site JS.
# Covers every signal DataDome's challenge script checks.
STEALTH_JS = r"""
(function() {
    'use strict';

    // 1. Remove webdriver flag
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

    // 2. Full chrome runtime (absent in headless)
    if (!window.chrome) window.chrome = {};
    window.chrome.app = {
        isInstalled: false,
        InstallState: { DISABLED:'disabled', INSTALLED:'installed', NOT_INSTALLED:'not_installed' },
        RunningState: { CANNOT_RUN:'cannot_run', READY_TO_RUN:'ready_to_run', RUNNING:'running' },
        getDetails: function(){return null;},
        getIsInstalled: function(){return false;},
        installState: function(cb){cb('not_installed');},
    };
    window.chrome.csi = function(){
        return {onloadT:Date.now(),pageT:3974.96,startE:Date.now()-4000,tran:15};
    };
    window.chrome.loadTimes = function(){
        var now = Date.now()/1000;
        return {requestTime:now-2,startLoadTime:now-1.8,commitLoadTime:now-1.5,
                finishDocumentLoadTime:now-0.5,finishLoadTime:now-0.1,
                firstPaintTime:now-0.8,firstPaintAfterLoadTime:0,
                navigationType:'Other',wasFetchedViaSpdy:true,wasNpnNegotiated:true,
                npnNegotiatedProtocol:'h2',wasAlternateProtocolAvailable:false,
                connectionInfo:'h2'};
    };
    window.chrome.runtime = {
        connect:function(){},sendMessage:function(){},id:undefined,
        OnInstalledReason:{CHROME_UPDATE:'chrome_update',INSTALL:'install',
            SHARED_MODULE_UPDATE:'shared_module_update',UPDATE:'update'},
        PlatformOs:{ANDROID:'android',CROS:'cros',LINUX:'linux',MAC:'mac',
            OPENBSD:'openbsd',WIN:'win'},
    };

    // 3. Realistic navigator.plugins (empty array in headless)
    var pluginData = [
        {name:'PDF Viewer',            filename:'internal-pdf-viewer',description:'Portable Document Format'},
        {name:'Chrome PDF Viewer',     filename:'internal-pdf-viewer',description:'Portable Document Format'},
        {name:'Chromium PDF Viewer',   filename:'internal-pdf-viewer',description:'Portable Document Format'},
        {name:'Microsoft Edge PDF Viewer',filename:'internal-pdf-viewer',description:'Portable Document Format'},
        {name:'WebKit built-in PDF',   filename:'internal-pdf-viewer',description:'Portable Document Format'},
    ];
    var pluginArr = Object.create(PluginArray.prototype);
    pluginData.forEach(function(pd, i) {
        var mime = Object.create(MimeType.prototype);
        Object.defineProperties(mime, {
            type:{value:'application/pdf',enumerable:true},
            suffixes:{value:'pdf',enumerable:true},
            description:{value:pd.description,enumerable:true},
        });
        var plugin = Object.create(Plugin.prototype);
        Object.defineProperties(plugin, {
            name:{value:pd.name,enumerable:true},
            filename:{value:pd.filename,enumerable:true},
            description:{value:pd.description,enumerable:true},
            length:{value:1,enumerable:true},
        });
        plugin[0] = mime;
        pluginArr[i] = plugin;
    });
    Object.defineProperty(pluginArr, 'length', {value:pluginData.length});
    Object.defineProperty(navigator, 'plugins', {get:function(){return pluginArr;}});

    // 4. navigator properties
    Object.defineProperty(navigator, 'languages',          {get:function(){return ['en-US','en'];}});
    Object.defineProperty(navigator, 'vendor',             {get:function(){return 'Google Inc.';}});
    Object.defineProperty(navigator, 'platform',           {get:function(){return 'Win32';}});
    Object.defineProperty(navigator, 'hardwareConcurrency',{get:function(){return 8;}});
    Object.defineProperty(navigator, 'deviceMemory',       {get:function(){return 8;}});
    Object.defineProperty(navigator, 'maxTouchPoints',     {get:function(){return 0;}});
    Object.defineProperty(navigator, 'doNotTrack',         {get:function(){return null;}});

    // 5. Permissions API — headless returns 'denied' for notifications
    var _origQuery = window.navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.__proto__.query = function(params) {
        if (params.name === 'notifications') {
            return Promise.resolve({state: Notification.permission, onchange: null});
        }
        return _origQuery(params);
    };

    // 6. WebGL — headless shows "Google SwiftShader" (dead giveaway)
    function patchWebGL(Cls) {
        var orig = Cls.prototype.getParameter;
        Cls.prototype.getParameter = function(p) {
            if (p === 37445) return 'Intel Inc.';
            if (p === 37446) return 'Intel Iris OpenGL Engine';
            return orig.call(this, p);
        };
    }
    if (typeof WebGLRenderingContext  !== 'undefined') patchWebGL(WebGLRenderingContext);
    if (typeof WebGL2RenderingContext !== 'undefined') patchWebGL(WebGL2RenderingContext);

    // 7. Canvas fingerprint — add imperceptible noise so hash differs from headless
    var _origToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type) {
        var ctx = this.getContext && this.getContext('2d');
        if (ctx) {
            try {
                var img = ctx.getImageData(0, 0, this.width||1, this.height||1);
                img.data[0] ^= 1;
                ctx.putImageData(img, 0, 0);
            } catch(e) {}
        }
        return _origToDataURL.apply(this, arguments);
    };

    // 8. Audio context fingerprint noise
    try {
        var _origGetChannelData = AudioBuffer.prototype.getChannelData;
        AudioBuffer.prototype.getChannelData = function() {
            var arr = _origGetChannelData.apply(this, arguments);
            for (var i = 0; i < arr.length; i += 100) {
                arr[i] += (Math.random() - 0.5) * 0.0000001;
            }
            return arr;
        };
    } catch(e) {}

    // 9. Window outer dimensions (0x0 in headless)
    if (!window.outerWidth || window.outerWidth === 0) {
        Object.defineProperty(window, 'outerWidth',  {get:function(){return window.innerWidth;}});
        Object.defineProperty(window, 'outerHeight', {get:function(){return window.innerHeight + 88;}});
    }

    // 10. Screen
    Object.defineProperty(screen, 'colorDepth', {get:function(){return 24;}});
    Object.defineProperty(screen, 'pixelDepth', {get:function(){return 24;}});

    // 11. iframe webdriver leak
    try {
        var _iframeDesc = Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype, 'contentWindow');
        Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
            get: function() {
                var w = _iframeDesc.get.call(this);
                if (w) {
                    try {
                        Object.defineProperty(w.navigator, 'webdriver', {get:function(){return undefined;}});
                    } catch(e) {}
                }
                return w;
            }
        });
    } catch(e) {}

    // 12. Battery API — not present in headless
    if (!navigator.getBattery) {
        navigator.getBattery = function() {
            return Promise.resolve({
                charging:true, chargingTime:0, dischargingTime:Infinity, level:1,
                addEventListener:function(){}, removeEventListener:function(){},
            });
        };
    }

    // 13. Network info
    if (navigator.connection) {
        try {
            Object.defineProperty(navigator.connection, 'rtt',           {get:function(){return 50;}});
            Object.defineProperty(navigator.connection, 'downlink',      {get:function(){return 10;}});
            Object.defineProperty(navigator.connection, 'effectiveType', {get:function(){return '4g';}});
        } catch(e) {}
    }
})();
"""


class PlaywrightResponse:
    """Mimics httpx.Response so spider.py/scraper.py need zero changes."""
    def __init__(self, status, text, headers, history=None):
        self.status_code = status
        self.text        = text
        self.headers     = headers
        self.history     = history or []


class PlaywrightWorker:
    """
    Runs a single Playwright browser in a daemon thread (own asyncio loop).
    Applies stealth to look like a real Chrome browser to DataDome.
    Stays alive across many requests — DataDome solved once per session.
    """

    MJCS_HOME   = 'https://casesearch.courts.state.md.us/casesearch/inquiry-index.jsp'
    MJCS_SEARCH = 'https://casesearch.courts.state.md.us/casesearch/inquiry-search.jsp'

    def __init__(self, session_id: str):
        self.session_id   = session_id
        self._req_queue   = thread_queue.Queue()
        self._ready_event = threading.Event()
        self._error       = None
        self._stealth_fn  = None
        self._thread      = threading.Thread(
            target=self._thread_main, daemon=True, name=f'pw-{session_id}'
        )
        self._thread.start()
        self._ready_event.wait(timeout=120)
        if self._error:
            raise RuntimeError(f'[{session_id}] Startup failed: {self._error}')

    def _proxy_cfg(self):
        user = os.getenv('PROXY_USERNAME', 'oehwhsaf-rotate')
        pw   = os.getenv('PROXY_PASSWORD', 'w03c3ohqnnbp')
        host = os.getenv('PROXY_HOST',     'p.webshare.io')
        port = os.getenv('PROXY_PORT',     '80')
        return {'server': f'http://{host}:{port}', 'username': user, 'password': pw}

    def _thread_main(self):
        try:
            asyncio.run(self._async_main())
        except Exception as e:
            self._error = str(e)
            self._ready_event.set()

    async def _async_main(self):
        from playwright.async_api import async_playwright

        # Load playwright-stealth if installed (pip install playwright-stealth)
        try:
            from playwright_stealth import stealth_async
            self._stealth_fn = stealth_async
            logger.info(f'[{self.session_id}] playwright-stealth library active')
        except ImportError:
            logger.info(f'[{self.session_id}] playwright-stealth not found, using manual stealth only')

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                proxy=self._proxy_cfg(),
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-ipc-flooding-protection',
                    '--enable-features=NetworkService,NetworkServiceInProcess',
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                viewport={'width': 1280, 'height': 800},
                locale='en-US',
                timezone_id='America/New_York',
                color_scheme='light',
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                },
            )

            # Apply our comprehensive stealth at context level
            await context.add_init_script(STEALTH_JS)

            # Solve DataDome + disclaimer
            await self._renew(context)
            self._ready_event.set()
            logger.info(f'[{self.session_id}] Worker ready')

            while True:
                try:
                    item = self._req_queue.get(timeout=0.2)
                except thread_queue.Empty:
                    continue
                if item is None:
                    break
                method, url, data, headers, result_q = item
                try:
                    result = await self._do_request(context, method, url, data, headers)
                    result_q.put(result)
                except Exception as e:
                    result_q.put({'error': str(e)})

            await browser.close()

    async def _renew(self, context, attempt=1):
        """
        Open a real page, apply stealth, navigate to MJCS.
        DataDome's JS challenge runs in this context and — with stealth patches —
        should see a convincing real-browser fingerprint and issue a valid cookie.
        """
        if attempt > 5:
            raise RuntimeError(f'[{self.session_id}] Could not pass DataDome after 5 attempts')

        page = await context.new_page()
        try:
            # Apply playwright-stealth on top of our context-level JS
            if self._stealth_fn:
                await self._stealth_fn(page)

            # Move mouse — DataDome tracks whether the cursor ever moved
            await page.mouse.move(
                random.randint(300, 900), random.randint(150, 550)
            )

            logger.info(f'[{self.session_id}] Navigating to MJCS (attempt {attempt})...')
            await page.goto(self.MJCS_HOME, wait_until='networkidle', timeout=60000)

            # Give DataDome's challenge script time to fully run + fingerprint
            await asyncio.sleep(7 + random.uniform(1, 3))

            # Small human-like mouse movements while "reading"
            for _ in range(random.randint(2, 4)):
                await page.mouse.move(
                    random.randint(200, 1000), random.randint(100, 650)
                )
                await asyncio.sleep(random.uniform(0.4, 1.0))

            content = await page.content()
            cookies = await context.cookies()
            dd = next((c for c in cookies if c['name'] == 'datadome'), None)
            if dd:
                logger.info(f'[{self.session_id}] DataDome cookie: {dd["value"][:50]}...')

            if self._is_blocked(content):
                logger.warning(f'[{self.session_id}] Still blocked — retry {attempt + 1} in {25*attempt}s')
                await page.close()
                await asyncio.sleep(25 * attempt)
                return await self._renew(context, attempt + 1)

            # Accept disclaimer checkbox if shown
            try:
                disc = page.locator('input[name="disclaimer"]')
                if await disc.count() > 0:
                    await disc.scroll_into_view_if_needed()
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    await disc.click()
                    await page.wait_for_load_state('networkidle', timeout=15000)
                    await asyncio.sleep(2)
                    logger.info(f'[{self.session_id}] Disclaimer accepted')
            except Exception:
                pass

            logger.info(f'[{self.session_id}] Renew OK — DataDome solved')

        finally:
            await page.close()

    async def _do_request(self, context, method, url, data, headers):
        """
        POST (spider search): submit via real page form so DataDome JS fires.
        GET (scraper case detail): use context.request.fetch() with shared cookies.
        """
        if method.upper() == 'POST':
            return await self._post_via_page(context, url, data, headers)

        resp = await context.request.fetch(
            url, method=method, headers=headers or {}, timeout=135000
        )
        text = await resp.text()
        if self._is_blocked(text):
            logger.warning(f'[{self.session_id}] DataDome on GET — re-solving')
            await self._renew(context)
            resp = await context.request.fetch(
                url, method=method, headers=headers or {}, timeout=135000
            )
            text = await resp.text()
        return {'status': resp.status, 'text': text, 'headers': dict(resp.headers)}

    async def _post_via_page(self, context, url, data, headers):
        """
        Submit a POST form via a real browser page navigated from MJCS origin.
        This ensures DataDome cookies are attached and the JS challenge fires
        with the correct referrer/origin context.
        """
        page = await context.new_page()
        if self._stealth_fn:
            await self._stealth_fn(page)
        try:
            # Start from MJCS origin so referrer + cookies are correct
            await page.goto(self.MJCS_HOME, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(random.uniform(1, 2))

            # Build hidden form fields JS
            fields_js = ''
            if data:
                items = data.items() if hasattr(data, 'items') else data
                for k, v in items:
                    k = str(k).replace("'", "\\'")
                    v = str(v).replace("'", "\\'")
                    fields_js += (
                        f"var i=document.createElement('input');"
                        f"i.type='hidden';i.name='{k}';i.value='{v}';"
                        f"f.appendChild(i);"
                    )

            # Inject and submit form from within the MJCS origin
            await page.evaluate(f"""
                (function() {{
                    var f = document.createElement('form');
                    f.method = 'POST';
                    f.action = '{url}';
                    {fields_js}
                    document.body.appendChild(f);
                    f.submit();
                }})();
            """)

            await page.wait_for_load_state('networkidle', timeout=45000)
            await asyncio.sleep(2)
            content = await page.content()

            if self._is_blocked(content):
                logger.warning(f'[{self.session_id}] DataDome on POST — re-solving')
                await page.close()
                await self._renew(context)
                # One retry
                page = await context.new_page()
                if self._stealth_fn:
                    await self._stealth_fn(page)
                await page.goto(self.MJCS_HOME, wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(1)
                await page.evaluate(f"""
                    (function() {{
                        var f = document.createElement('form');
                        f.method = 'POST';
                        f.action = '{url}';
                        {fields_js}
                        document.body.appendChild(f);
                        f.submit();
                    }})();
                """)
                await page.wait_for_load_state('networkidle', timeout=45000)
                content = await page.content()

            return {'status': 200, 'text': content, 'headers': {}}
        finally:
            await page.close()

    @staticmethod
    def _is_blocked(text):
        return (
            'Access is temporarily restricted' in text or
            ('datadome' in text.lower() and 'Please enable JS' in text) or
            ('captcha-delivery.com' in text)
        )

    def sync_request(self, method, url, data=None, headers=None, timeout=180):
        """Blocking — safe to call from any thread."""
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
    """Async wrapper around PlaywrightWorker — usable from trio via to_thread."""

    def __init__(self, session_id: str):
        self.session_id = session_id
        self._worker    = None

    def start_sync(self):
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
        pass  # Playwright handles renew internally

    async def aclose(self):
        if self._worker:
            import trio
            await trio.to_thread.run_sync(self._worker.shutdown, abandon_on_cancel=True)


class PlaywrightSessionPool:
    """Drop-in replacement for AsyncSessionPool using Playwright browsers."""

    def __init__(self, concurrency: int):
        self.concurrency = concurrency
        self._sessions   = []
        self._send_ch    = None
        self._recv_ch    = None

    async def start_all(self):
        import trio
        self._send_ch, self._recv_ch = trio.open_memory_channel(
            max_buffer_size=self.concurrency
        )
        for i in range(self.concurrency):
            sid = f'SP{i:02d}'
            s = PlaywrightSession(sid)
            logger.info(f'Starting Playwright session {sid}...')
            await trio.to_thread.run_sync(s.start_sync, abandon_on_cancel=True)
            self._sessions.append(s)
            self._send_ch.send_nowait(s)
            if i < self.concurrency - 1:
                await trio.sleep(20)  # stagger to avoid hammering DataDome

    async def get(self):
        return await self._recv_ch.receive()

    def put_nowait(self, session):
        self._send_ch.send_nowait(session)

    async def close_all(self):
        import trio
        for s in self._sessions:
            await s.aclose()
