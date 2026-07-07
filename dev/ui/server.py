"""Dev-only web server for the orchestrator browser console.

Serves the static GUI (dev/ui/static) AND reverse-proxies ``/api/*`` to the orchestrator API, so
the browser only ever talks to this one origin — no CORS, and the orchestrator API stays untouched.

Run it (no dependencies — stdlib only):

    python dev/ui/server.py
    # or point at a non-default API / port:
    ORCH_API_BASE=http://localhost:8000 UI_PORT=8090 python dev/ui/server.py

Then open http://localhost:8090/.
"""

import os
import urllib.error
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

API_BASE = os.environ.get("ORCH_API_BASE", "http://localhost:8000").rstrip("/")
STATIC_DIR = Path(__file__).parent / "static"
PORT = int(os.environ.get("UI_PORT", "8090"))


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)  # type: ignore[arg-type]

    def do_GET(self) -> None:
        if self.path.startswith("/api/"):
            self._proxy("GET")
        else:
            super().do_GET()  # static files

    def do_POST(self) -> None:
        self._proxy("POST")

    def do_PATCH(self) -> None:
        self._proxy("PATCH")

    def do_PUT(self) -> None:
        self._proxy("PUT")

    def do_DELETE(self) -> None:
        self._proxy("DELETE")

    def _proxy(self, method: str) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else None
        req = urllib.request.Request(API_BASE + self.path, data=body, method=method)
        if content_type := self.headers.get("Content-Type"):
            req.add_header("Content-Type", content_type)
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
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
