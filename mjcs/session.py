"""
MJCS Session - handles authentication with Maryland Judiciary Case Search
Uses curl_cffi to impersonate Chrome TLS fingerprint (bypasses DataDome on residential IPs).
Supports ScraperAPI and ZenRows proxy services for IP ban bypass.
Proactively accepts the disclaimer on session start.
"""
from .config import config
import logging
import os
import time
import random
import json
import urllib.parse
import requests as std_requests
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

logger = logging.getLogger("mjcs")

ZENROWS_API_URL = "https://api.zenrows.com/v1/"


class RequestTimeout(Exception):
    pass


class Forbidden(Exception):
    pass


class ZenRowsSession:
    """
    Drop-in session replacement that routes every MJCS request through
    ZenRows' residential proxy network via their Scraper API.

    ZenRows 'session' parameter pins all requests to the same residential IP
    so that MJCS cookie-based auth works across multiple requests.
    """

    def __init__(self, api_key, session_id=None):
        self.api_key = api_key
        self.session_id = session_id or random.randint(1, 99999)
        self._cookies = {}  # manually track MJCS cookies across calls
        logger.info(f"Using ZenRows residential proxy (session {self.session_id})")

    def _zenrows_request(self, method, url, data=None, timeout=None, **kwargs):
        params = {
            "apikey": self.api_key,
            "url": url,
            "premium_proxy": "true",   # residential IPs — bypasses DataDome
            "js_render": "false",       # HTML is static, JS render not needed
            "session": str(self.session_id),
            "original_status": "true",  # get real HTTP status, not ZenRows wrapper
        }

        # Forward any cookies we've accumulated from prior MJCS responses
        if self._cookies:
            cookie_header = "; ".join(f"{k}={v}" for k, v in self._cookies.items())
            params["custom_headers"] = json.dumps({"Cookie": cookie_header})

        # POST data must be URL-encoded and passed as post_data param
        if method.upper() == "POST" and data:
            params["post_data"] = urllib.parse.urlencode(data)

        try:
            resp = std_requests.get(
                ZENROWS_API_URL,
                params=params,
                timeout=timeout or 60,
            )
        except std_requests.Timeout:
            raise RequestTimeout()

        # Harvest Set-Cookie headers returned by ZenRows from the upstream site
        for raw in resp.headers.getlist("Set-Cookie") if hasattr(resp.headers, "getlist") else []:
            for part in raw.split(","):
                kv = part.strip().split(";")[0]
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    self._cookies[k.strip()] = v.strip()

        # Also parse cookies from the ZenRows-specific response header if present
        zr_cookies = resp.headers.get("Zr-Original-Set-Cookie", "")
        if zr_cookies:
            for part in zr_cookies.split(","):
                kv = part.strip().split(";")[0]
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    self._cookies[k.strip()] = v.strip()

        if resp.status_code == 403:
            raise Forbidden()

        return resp

    def get(self, url, **kwargs):
        return self._zenrows_request("GET", url, **kwargs)

    def post(self, url, data=None, **kwargs):
        return self._zenrows_request("POST", url, data=data, **kwargs)

    def request(self, method, url, **kwargs):
        data = kwargs.pop("data", None)
        return self._zenrows_request(method, url, data=data, **kwargs)

    @property
    def history(self):
        return []


def _build_session(scraperapi_session_id=None):
    import urllib3
    proxy = os.getenv("SCRAPER_PROXY")
    scraperapi_key = os.getenv("SCRAPERAPI_KEY")

    if scraperapi_key:
        session_id = scraperapi_session_id or random.randint(1, 99999)
        proxy_url = f"http://scraperapi.session_number={session_id}:{scraperapi_key}@proxy-server.scraperapi.com:8001"
        logger.info(f"Using ScraperAPI proxy (session {session_id}) for IP ban bypass")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        session = std_requests.Session()
        session.proxies = {"http": proxy_url, "https": proxy_url}
        session.verify = False
        return session

    session = cffi_requests.Session(impersonate="chrome110")
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}
        logger.info(f"Using proxy: {proxy[:30]}...")
    return session


class MjcsSession:
    def __init__(self):
        self.requests = 0
        self._scraperapi_session_id = None
        self._zenrows_session_id = None
        self.new_session()

    def new_session(self):
        self._scraperapi_session_id = random.randint(1, 99999)
        self._zenrows_session_id = random.randint(1, 99999)

        zenrows_key = os.getenv("ZENROWS_KEY")
        if zenrows_key:
            self.session = ZenRowsSession(zenrows_key, session_id=self._zenrows_session_id)
        else:
            self.session = _build_session(scraperapi_session_id=self._scraperapi_session_id)

        # Proactively accept disclaimer so the session is ready to search
        try:
            self.renew()
        except Exception as e:
            logger.warning(f"Could not pre-accept disclaimer: {e}")

    def _needs_disclaimer(self, response):
        """Return True if the response is asking us to accept the disclaimer."""
        history = getattr(response, "history", []) or []
        for r in history:
            loc = r.headers.get("location", "")
            if "inquiry-index.jsp" in loc or "inquiry-search.jsp" in loc:
                return True
        text = response.text
        return ("disclaimer" in text.lower() and
                ("accept" in text.lower() or "agreement" in text.lower()) and
                "inquirySearch" not in text)

    def request(self, *args, i=1, **kwargs):
        if i > 5:
            raise Exception("Too many recursed requests")
        self.requests += 1
        kwargs.setdefault("timeout", config.QUERY_TIMEOUT)
        response = self.session.request(*args, **kwargs)

        if self._needs_disclaimer(response):
            logger.debug(f"Renewing session (disclaimer detected, attempt {i})...")
            if i >= 3:
                # Hard reset: build a brand new HTTP session and re-accept disclaimer
                wait = 30 * (i - 2)  # Back-off: 30s, 60s, 90s
                logger.info(f"Hard resetting session after repeated disclaimers (waiting {wait}s)...")
                time.sleep(wait)
                self.new_session()
            else:
                time.sleep(5)
                self.renew()
            return self.request(*args, i=i + 1, **kwargs)
        return response

    def renew(self):
        self.requests += 1
        response = self.session.get(
            f"{config.MJCS_BASE_URL}/inquiry-index.jsp",
            timeout=config.QUERY_TIMEOUT
        )
        soup = BeautifulSoup(response.text, "html.parser")
        disclaimer_input = soup.find("input", {"name": "disclaimer"})
        if not disclaimer_input:
            # No disclaimer form — session may already be authenticated
            logger.info("Disclaimer accepted successfully")
            return response

        disclaimer_token = disclaimer_input.get("value")
        self.requests += 1
        response = self.session.post(
            f"{config.MJCS_BASE_URL}/processDisclaimer.jis",
            data={"disclaimer": disclaimer_token},
            timeout=config.QUERY_TIMEOUT,
        )

        if response.status_code != 200 or self._needs_disclaimer(response):
            err = f"Failed to accept disclaimer: code={response.status_code}"
            logger.error(err)
            raise Exception(err)

        logger.info("Disclaimer accepted successfully")
        return response
