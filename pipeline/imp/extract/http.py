"""HTTP client for NSE/AMFI with browser headers, cookie warm-up, retries and backoff.

NSE blocks many datacentre IPs and expects a browser-like session; run extraction from a
home/office network if downloads fail with 401/403.
"""
from __future__ import annotations

import time

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


class Client:
    def __init__(self, pause: float = 1.0, retries: int = 3, timeout: int = 30):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.pause, self.retries, self.timeout = pause, retries, timeout
        self._warmed = False

    def _warm(self) -> None:
        if not self._warmed:
            try:  # NSE sets cookies on the home page; API calls need them
                self.session.get("https://www.nseindia.com/", timeout=self.timeout)
            except requests.RequestException:
                pass
            self._warmed = True

    def get(self, url: str, nse: bool = True) -> bytes | None:
        """Body bytes, or None for 404 (holiday / not published). Raises after repeated failures."""
        if nse:
            self._warm()
        last = None
        for attempt in range(self.retries):
            try:
                r = self.session.get(url, timeout=self.timeout)
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                time.sleep(self.pause)
                return r.content
            except requests.RequestException as e:
                last = e
                time.sleep(self.pause * 2 ** attempt)
        raise RuntimeError(f"GET {url} failed after {self.retries} attempts: {last}")
