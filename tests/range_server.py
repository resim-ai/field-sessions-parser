"""S3 stand-ins for the range file: an in-process opener and a real HTTP server with Range support."""

from __future__ import annotations

import re
import threading
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RANGE_RE = re.compile(r"bytes=(\d+)-(\d*)")


def parse_range(value: str | None, length: int) -> tuple[int, int] | None:
    if not value:
        return None
    match = RANGE_RE.fullmatch(value.strip())
    if not match:
        return None
    start = int(match.group(1))
    end = int(match.group(2)) if match.group(2) else length - 1
    return start, min(end, length - 1)


class FakeOpener:
    def __init__(
        self,
        data: bytes,
        *,
        etag: str = '"fixture-etag"',
        ignore_range: bool = False,
        fail_status: int | None = None,
        fail_times: int | None = 0,
    ):
        self.data = data
        self.etag = etag
        self.ignore_range = ignore_range
        self.fail_status = fail_status
        self.fail_times = fail_times
        self.failed = 0
        self.calls: list[tuple[int, int] | None] = []

    def __call__(self, url: str, headers: Mapping[str, str]) -> tuple[int, Mapping[str, str], bytes]:
        wanted = parse_range(headers.get("Range"), len(self.data))
        self.calls.append(wanted)
        if self.fail_status is not None and (self.fail_times is None or self.failed < self.fail_times):
            self.failed += 1
            return self.fail_status, {"ETag": self.etag}, b""
        if self.ignore_range or wanted is None:
            return 200, {"ETag": self.etag, "Content-Length": str(len(self.data))}, self.data
        start, end = wanted
        if start >= len(self.data):
            return 416, {"Content-Range": f"bytes */{len(self.data)}", "ETag": self.etag}, b""
        body = self.data[start : end + 1]
        return 206, {"Content-Range": f"bytes {start}-{end}/{len(self.data)}", "ETag": self.etag}, body


class RangeServer:
    """Serves one blob at every path over HTTP with Range support, for the urllib opener."""

    def __init__(
        self,
        data: bytes,
        *,
        etag: str = '"server-etag"',
        status: int | None = None,
        truncate_responses: int = 0,
    ):
        self.data = data
        self.etag = etag
        self.status = status
        self.requests: list[str | None] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                range_header = self.headers.get("Range")
                server.requests.append(range_header)
                if server.status is not None:
                    self.send_response(server.status)
                    self.end_headers()
                    return
                wanted = parse_range(range_header, len(server.data))
                if wanted is None:
                    body = server.data
                    self.send_response(200)
                else:
                    start, end = wanted
                    body = server.data[start : end + 1]
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {start}-{end}/{len(server.data)}")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("ETag", server.etag)
                self.end_headers()
                if len(server.requests) <= truncate_responses:
                    self.wfile.write(body[: len(body) // 2])
                    self.close_connection = True
                else:
                    self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> RangeServer:
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()
