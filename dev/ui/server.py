"""Dev-only web server for the orchestrator browser console.

Serves the static GUI (dev/ui/static) AND reverse-proxies ``/api/*`` to the orchestrator API, so
the browser only ever talks to this one origin — no CORS, and the orchestrator API stays untouched.

Run it (no dependencies — stdlib only):

    python dev/ui/server.py
    # or point at a non-default API / port:
    ORCH_API_BASE=http://localhost:8000 UI_PORT=8090 python dev/ui/server.py

Then open http://localhost:8090/.

Access is gated by HTTP Basic auth when ``UI_PASSWORD`` is set (user ``UI_USERNAME``, default
``admin``); the browser re-sends the credentials on every same-origin ``fetch``, so the static GUI
needs no changes. With ``UI_PASSWORD`` unset the console is open (local-only use).

``UI_API_KEY`` is the orchestrator API key the proxy adds as ``X-API-Key`` to every ``/api`` call
— server-side, so the browser never sees it. Leave it unset when the API has no keys configured.
"""

import base64
import hmac
import os
import urllib.error
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

API_BASE = os.environ.get("ORCH_API_BASE", "http://localhost:8000").rstrip("/")
STATIC_DIR = Path(__file__).parent / "static"
PORT = int(os.environ.get("UI_PORT", "8090"))
USERNAME = os.environ.get("UI_USERNAME", "admin")
PASSWORD = os.environ.get("UI_PASSWORD", "")
API_KEY = os.environ.get("UI_API_KEY", "")
EXPECTED_AUTH = (
    "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode() if PASSWORD else ""
)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)  # type: ignore[arg-type]

    def do_GET(self) -> None:
        if not self._authorized():
            return
        if self.path.startswith("/api/"):
            self._proxy("GET")
        else:
            super().do_GET()  # static files

    def do_HEAD(self) -> None:
        if self._authorized():
            super().do_HEAD()

    def do_POST(self) -> None:
        self._proxy("POST")

    def do_PATCH(self) -> None:
        self._proxy("PATCH")

    def do_PUT(self) -> None:
        self._proxy("PUT")

    def do_DELETE(self) -> None:
        self._proxy("DELETE")

    def _authorized(self) -> bool:
        """True if auth is off or the request carries the right Basic credentials; else send 401."""
        given = self.headers.get("Authorization", "")
        if not EXPECTED_AUTH or hmac.compare_digest(given.encode(), EXPECTED_AUTH.encode()):
            return True
        data = b'{"detail": "authentication required"}'
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="orchestrator console", charset="UTF-8"')
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        return False

    def _proxy(self, method: str) -> None:
        if method != "GET" and not self._authorized():  # do_GET already checked
            return
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else None
        req = urllib.request.Request(API_BASE + self.path, data=body, method=method)
        if content_type := self.headers.get("Content-Type"):
            req.add_header("Content-Type", content_type)
        if API_KEY:
            req.add_header("X-API-Key", API_KEY)
        try:
            with urllib.request.urlopen(req) as resp:
                self._relay(resp.status, resp.headers.get_content_type(), resp.read())
        except urllib.error.HTTPError as e:  # forward the API's own 4xx/5xx + body
            self._relay(e.code, "application/json", e.read())
        except urllib.error.URLError as e:
            msg = f'{{"detail": "orchestrator API unreachable at {API_BASE}: {e.reason}"}}'
            self._relay(502, "application/json", msg.encode())

    def _relay(self, status: int, content_type: str, data: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type or "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args: object) -> None:  # keep the console quiet
        pass


if __name__ == "__main__":
    print(f"Orchestrator dev console → http://localhost:{PORT}/   (/api → {API_BASE})")
    if not PASSWORD:
        print("WARNING: UI_PASSWORD is not set — the console is open to anyone who can reach it.")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
