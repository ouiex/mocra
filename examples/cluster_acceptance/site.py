#!/usr/bin/env python3
"""Deterministic page/detail HTTP corpus with request audit records."""

import html
import json
import math
import os
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


MANIFEST_PATH = Path(os.environ.get("MOCRA_MANIFEST", "/control/manifest.json"))
AUDIT_PATH = Path(os.environ.get("MOCRA_HTTP_AUDIT", "/output/http-audit.jsonl"))
AUDIT_LOCK = threading.Lock()
ROUTE = re.compile(r"^/(page|detail)/([A-Za-z0-9_-]+)/([0-9]+)$")


def phase_delay(phase):
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    delay = manifest.get("delay_seconds", 0.04) if manifest.get("phase") == phase else 0.04
    if isinstance(delay, bool) or not isinstance(delay, (int, float)):
        raise ValueError("delay_seconds must be a number")
    if not math.isfinite(delay) or delay < 0:
        raise ValueError("delay_seconds must be finite and nonnegative")
    return float(delay)


def corpus_body(kind, phase, item_id):
    phase = html.escape(phase, quote=True)
    item_id = html.escape(str(item_id), quote=True)
    link = f"<a class='detail' href='/detail/{phase}/{item_id}'>Detail</a>" if kind == "page" else ""
    document = (
        "<!doctype html><html><head><meta charset='utf-8'><title>HTTP corpus</title>"
        "</head><body>"
        f"<article data-kind='{kind}' data-id='{item_id}' data-phase='{phase}'>"
        f"<span class='value'>value-{item_id}</span>{link}</article>"
        "</body></html>"
    ).encode("utf-8")
    size = 70 * 1024 if phase == "blob" else 1024
    padding = b"<!--" + b"x" * max(0, size - len(document) - 7) + b"-->"
    return document.replace(b"</body>", padding + b"</body>")


def audit(record):
    record = dict(record, time_ns=time.time_ns())
    with AUDIT_LOCK:
        with AUDIT_PATH.open("a", encoding="utf-8") as output:
            output.write(json.dumps(record, separators=(",", ":")) + "\n")
            output.flush()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/health":
            self.send_body(200, b"ok\n", "text/plain; charset=utf-8")
            return
        match = ROUTE.fullmatch(path)
        kind, phase, item_id = match.groups() if match else (None, None, None)
        record = {
            "request_token": uuid.uuid4().hex, "remote_ip": self.client_address[0],
            "path": path, "phase": phase, "id": item_id, "kind": kind,
        }
        audit(dict(record, event="begin", status="started"))
        status = 200 if match else 404
        try:
            if match:
                try:
                    delay = phase_delay(phase)
                except (OSError, ValueError, TypeError, AttributeError) as error:
                    status = 500
                    body = f"Invalid corpus manifest: {error}\n".encode("utf-8")
                else:
                    time.sleep(delay)
                    body = corpus_body(kind, phase, item_id)
            else:
                body = b"Unknown corpus route\n"
            content_type = "text/html; charset=utf-8" if status == 200 else "text/plain; charset=utf-8"
            self.send_body(status, body, content_type)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            status = "aborted"
        finally:
            audit(dict(record, event="end", status=status))

    def send_body(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def log_message(self, _format, *args):
        pass


def main():
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
