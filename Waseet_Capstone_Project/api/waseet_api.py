"""
The Waseet customer API, running on your own machine.

    from waseet_api import start_api
    BASE_URL = start_api()

Then:

    GET {BASE_URL}/health                       no key needed
    GET {BASE_URL}/customers?page=1             needs  X-API-Key: waseet-demo-key

Nothing in this file is part of the capstone. Read it if you want to see what the
other side of an HTTP call looks like; ignore it otherwise.

Three things about it are deliberate, because they are the three things that break
a naive client:

  - the key goes in a header, not the query string
  - the answer is an envelope, not a list, and the records are under "customers"
  - the first attempt at every third page is refused with 429

The third one is not random. Ask again and it succeeds, so a client with a retry
finishes the pull and a client without one loses a third of the customers and
never notices.
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

API_KEY = "waseet-demo-key"
PAGE_SIZE = 25

_HERE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(_HERE, "customers.json"), encoding="utf-8") as _handle:
    CUSTOMERS = json.load(_handle)

TOTAL_RECORDS = len(CUSTOMERS)
_seen_pages = set()
_lock = threading.Lock()


class _Handler(BaseHTTPRequestHandler):

    def _send(self, status, payload, extra_headers=None):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if parsed.path == "/health":
            self._send(200, {"status": "up", "customers": TOTAL_RECORDS})
            return

        if parsed.path != "/customers":
            self._send(404, {"error": "no such endpoint", "path": parsed.path})
            return

        if self.headers.get("X-API-Key") != API_KEY:
            self._send(401, {"error": "missing or wrong API key",
                             "hint": "send it in the X-API-Key header"})
            return

        page = int(query.get("page", ["1"])[0])
        total_pages = (TOTAL_RECORDS + PAGE_SIZE - 1) // PAGE_SIZE

        if page < 1 or page > total_pages:
            self._send(404, {"error": "page out of range",
                             "total_pages": total_pages})
            return

        with _lock:
            first_attempt = page % 3 == 0 and page not in _seen_pages
            _seen_pages.add(page)
        if first_attempt:
            self._send(429, {"error": "too many requests", "retry_after": 1},
                       {"Retry-After": "1"})
            return

        start = (page - 1) * PAGE_SIZE
        self._send(200, {
            "page": page,
            "page_size": PAGE_SIZE,
            "total_pages": total_pages,
            "total_records": TOTAL_RECORDS,
            "has_more": page < total_pages,
            "customers": CUSTOMERS[start:start + PAGE_SIZE],
        })

    def log_message(self, *args):
        pass


def start_api():
    """Start the API on a free port and return its base URL."""
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return "http://127.0.0.1:%d" % server.server_address[1]


def reset_rate_limit():
    """Forget which pages have been asked for, so the 429s happen again."""
    with _lock:
        _seen_pages.clear()
