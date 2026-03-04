"""
MJCS Session - handles authentication with Maryland Judiciary Case Search
Uses curl_cffi to impersonate Chrome TLS fingerprint (bypasses DataDome on residential IPs).
Proactively accepts the disclaimer on session start.
"""
from .config import config
import logging
import os
import time
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

logger = logging.getLogger("mjcs")

# Disclaimer trigger phrases (check both old and new UI wording)
DISCLAIMER_PHRASES = [
    "Acceptance of the following agreement is",
    "disclaimer",
]


class RequestTimeout(Exception):
    pass


class Forbidden(Exception):
    pass


def _build_session():
    proxy = os.getenv("SCRAPER_PROXY")
    scraperapi_key = os.getenv("SCRAPERAPI_KEY")
    if scraperapi_key:
        proxy = f"http://scraperapi:{scraperapi_key}@proxy-server.scraperapi.com:8001"
        logger.info("Using ScraperAPI for DataDome bypass")

    session = cffi_requests.Session(impersonate="chrome110")
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}
        logger.info(f"Using proxy: {proxy[:30]}...")
    return session


class MjcsSession:
    def __init__(self):
        self.requests = 0
        self.new_session()

    def new_session(self):
        self.session = _build_session()
        # Proactively accept disclaimer so the session is ready to search
        try:
            self.renew()
        except Exception as e:
            logger.warning(f"Could not pre-accept disclaimer: {e}")

    def _needs_disclaimer(self, response):
        """Return True if the response is asking us to accept the disclaimer."""
        if response.history:
            for r in response.history:
                loc = r.headers.get("location", "")
                if "inquiry-index.jsp" in loc or "inquiry-search.jsp" in loc:
                    return True
        text = response.text
        return ("disclaimer" in text.lower() and
                ("accept" in text.lower() or "agreement" in text.lower()) and
                "inquirySearch" not in text)

    def request(self, *args, i=1, **kwargs):
        if i > 2:
            raise Exception("Too many recursed requests")
        self.requests += 1
        kwargs.setdefault("timeout", config.QUERY_TIMEOUT)
        response = self.session.request(*args, **kwargs)

        if self._needs_disclaimer(response):
            logger.debug("Renewing session (disclaimer detected)...")
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
            logger.warning("No disclaimer token found - may already be authenticated")
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
