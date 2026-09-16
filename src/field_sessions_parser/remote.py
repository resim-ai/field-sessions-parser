"""Seekable range readers over S3, HTTP and local files with a block cache and request counters."""

from __future__ import annotations

import http.client
import io
import re
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import botocore.exceptions

BLOCK_SIZE = 4 * 1024 * 1024
MAX_CACHED_BLOCKS = 48
MAX_BLOCKS_PER_REQUEST = 16
HEAD_PROBE_BYTES = 4096
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
BACKOFF_INITIAL_S = 0.5
MAX_ATTEMPTS = 6
FATAL_S3_CODES = frozenset({"AccessDenied", "NoSuchKey", "NoSuchBucket", "InvalidObjectState"})
TRANSIENT_S3_CODES = frozenset(
    {
        "SlowDown",
        "RequestTimeout",
        "InternalError",
        "ServiceUnavailable",
        "Throttling",
        "RequestLimitExceeded",
    }
)
HTTP_TIMEOUT_S = 60

Opener = Callable[[str, Mapping[str, str]], tuple[int, Mapping[str, str], bytes]]


class SourceError(Exception):
    """The object cannot be read; the message never carries a query string or a signature."""


class TransientError(Exception):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Fetched:
    data: bytes
    total_size: int | None
    etag: str | None


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path, "", ""))


def parse_content_range(value: str | None) -> int | None:
    if not value or "/" not in value:
        return None
    total = value.rsplit("/", 1)[1].strip()
    return int(total) if total.isdigit() else None


def clean_etag(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().strip('"')


class RangeFile(io.IOBase):
    """A read-only seekable file whose bytes arrive in fixed blocks from a remote range source."""

    def __init__(
        self,
        *,
        block_size: int = BLOCK_SIZE,
        max_cached_blocks: int = MAX_CACHED_BLOCKS,
        max_blocks_per_request: int = MAX_BLOCKS_PER_REQUEST,
        head_probe_bytes: int = HEAD_PROBE_BYTES,
        max_attempts: int = MAX_ATTEMPTS,
        sleep: Callable[[float], None] = time.sleep,
    ):
        super().__init__()
        self._block_size = block_size
        self._max_cached_blocks = max_cached_blocks
        self._max_blocks_per_request = max_blocks_per_request
        self._head_probe_bytes = head_probe_bytes
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._blocks: OrderedDict[int, bytes] = OrderedDict()
        self._head = b""
        self._pos = 0
        self._size: int | None = None
        self._etag: str | None = None
        self.requests = 0
        self.bytes_fetched = 0

    def describe(self) -> str:
        raise NotImplementedError

    def _fetch(self, start: int, stop: int) -> Fetched:
        raise NotImplementedError

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            pos = offset
        elif whence == io.SEEK_CUR:
            pos = self._pos + offset
        elif whence == io.SEEK_END:
            pos = self.size + offset
        else:
            raise ValueError(f"unsupported whence {whence}")
        if pos < 0:
            raise ValueError("negative seek position")
        self._pos = pos
        return pos

    def read(self, n: int | None = -1) -> bytes:
        size = self.size
        if n is None or n < 0:
            n = max(0, size - self._pos)
        end = min(self._pos + n, size)
        if end <= self._pos:
            return b""
        data = self._read_range(self._pos, end)
        self._pos = end
        return data

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    @property
    def size(self) -> int:
        if self._size is None:
            self._probe()
        return self._size  # type: ignore[return-value]

    @property
    def etag(self) -> str | None:
        if self._size is None:
            self._probe()
        return self._etag

    @property
    def cached_blocks(self) -> int:
        return len(self._blocks)

    def _probe(self) -> None:
        fetched = self._fetch_retrying(0, self._head_probe_bytes - 1)
        self._head = fetched.data
        self._size = fetched.total_size if fetched.total_size is not None else len(fetched.data)
        self._etag = fetched.etag

    def _read_range(self, start: int, end: int) -> bytes:
        if end <= len(self._head):
            return self._head[start:end]
        first = start // self._block_size
        last = (end - 1) // self._block_size
        parts = []
        for index, block in self._iter_blocks(first, last):
            lo = start - index * self._block_size if index == first else 0
            hi = end - index * self._block_size if index == last else len(block)
            parts.append(block[lo:hi])
        return b"".join(parts)

    def _iter_blocks(self, first: int, last: int) -> Iterator[tuple[int, bytes]]:
        index = first
        while index <= last:
            if index in self._blocks:
                self._blocks.move_to_end(index)
                yield index, self._blocks[index]
                index += 1
                continue
            fetched = self._fetch_blocks(index, last)
            yield from fetched.items()
            index += len(fetched)

    def _fetch_blocks(self, index: int, last: int) -> dict[int, bytes]:
        run_end = index
        while (
            run_end + 1 <= last
            and (run_end + 1) not in self._blocks
            and run_end + 1 - index + 1 <= self._max_blocks_per_request
        ):
            run_end += 1
        start = index * self._block_size
        stop = min((run_end + 1) * self._block_size, self.size) - 1
        fetched = self._fetch_retrying(start, stop)
        if len(fetched.data) != stop - start + 1:
            raise SourceError(
                f"{self.describe()}: short read, wanted {stop - start + 1} bytes at {start}, "
                f"got {len(fetched.data)}"
            )
        blocks = {}
        for block_index in range(index, run_end + 1):
            offset = (block_index - index) * self._block_size
            blocks[block_index] = fetched.data[offset : offset + self._block_size]
            self._blocks[block_index] = blocks[block_index]
            self._blocks.move_to_end(block_index)
        while len(self._blocks) > self._max_cached_blocks:
            self._blocks.popitem(last=False)
        return blocks

    def _fetch_retrying(self, start: int, stop: int) -> Fetched:
        delay = BACKOFF_INITIAL_S
        last_error: TransientError | None = None
        for attempt in range(self._max_attempts):
            self.requests += 1
            try:
                fetched = self._fetch(start, stop)
            except TransientError as err:
                last_error = err
                if attempt + 1 < self._max_attempts:
                    self._sleep(delay)
                    delay *= 2
                continue
            if self._size is not None and fetched.total_size != self._size:
                raise SourceError(f"{self.describe()}: object size changed during range reads")
            if self._etag is not None and fetched.etag != self._etag:
                raise SourceError(f"{self.describe()}: object ETag changed during range reads")
            if self._etag is None:
                self._etag = fetched.etag
            self.bytes_fetched += len(fetched.data)
            return fetched
        raise SourceError(f"{self.describe()}: giving up after {self._max_attempts} attempts: {last_error}")


def http_range_size(headers: Mapping[str, str], start: int, stop: int) -> tuple[int, int]:
    match = re.fullmatch(r"bytes ([0-9]+)-([0-9]+)/([0-9]+)", headers.get("content-range", ""))
    if match is None:
        raise SourceError("HTTP 206 response has missing or invalid Content-Range")
    first, last, total = map(int, match.groups())
    if total <= start or first != start or last != min(stop, total - 1):
        raise SourceError("HTTP 206 response does not match the requested byte range")
    expected = last - first + 1
    length = headers.get("content-length")
    if length is not None and (not re.fullmatch(r"[0-9]+", length) or int(length) != expected):
        raise SourceError("HTTP 206 Content-Length does not match Content-Range")
    if headers.get("content-encoding", "identity").lower() != "identity":
        raise SourceError("HTTP 206 response has unsupported Content-Encoding")
    return total, expected


def urllib_opener(url: str, headers: Mapping[str, str]) -> tuple[int, Mapping[str, str], bytes]:
    request = urllib.request.Request(url, headers=dict(headers))
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
            response_headers = {key.lower(): value for key, value in response.headers.items()}
            if response.status != 206:
                return response.status, response_headers, b""
            requested = re.fullmatch(r"bytes=([0-9]+)-([0-9]+)", headers.get("Range", ""))
            if requested is None:
                raise SourceError("HTTP reader requires an explicit byte range")
            _, expected = http_range_size(response_headers, *map(int, requested.groups()))
            return response.status, response_headers, response.read(expected + 1)
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers.items()), b""
    except http.client.IncompleteRead:
        raise TransientError("HTTP response body was incomplete") from None
    except (urllib.error.URLError, TimeoutError, OSError) as err:
        raise TransientError(type(err).__name__) from None


class HttpRangeFile(RangeFile):
    def __init__(self, url: str, opener: Opener | None = None, **options):
        super().__init__(**options)
        self.url = url
        self._opener = opener or urllib_opener

    def describe(self) -> str:
        return redact_url(self.url)

    def _fetch(self, start: int, stop: int) -> Fetched:
        status, raw_headers, body = self._opener(self.url, {"Range": f"bytes={start}-{stop}"})
        headers = {key.lower(): value for key, value in raw_headers.items()}
        etag = clean_etag(headers.get("etag"))
        if status == 206:
            total, expected = http_range_size(headers, start, stop)
            if len(body) < expected:
                raise TransientError(
                    f"HTTP response body was incomplete: expected {expected}, got {len(body)}"
                )
            if len(body) > expected:
                raise SourceError(f"{self.describe()}: HTTP response body exceeds the requested range")
            return Fetched(body, total, etag)
        if status == 200:
            raise SourceError(f"{self.describe()}: HTTP server ignored Range; full-object reads are disabled")
        if status == 416:
            match = re.fullmatch(r"bytes \*/([0-9]+)", headers.get("content-range", ""))
            if match is None or start < int(match[1]):
                raise SourceError(f"{self.describe()}: invalid HTTP 416 Content-Range")
            total = int(match[1])
            return Fetched(b"", total, etag)
        if status in RETRY_STATUSES:
            raise TransientError(f"HTTP {status}", status)
        if status == 403:
            raise SourceError(
                f"{self.describe()}: HTTP 403 Forbidden; a presigned URL may have expired or the "
                "credentials may not allow reading the object"
            )
        if status == 404:
            raise SourceError(f"{self.describe()}: HTTP 404 Not Found")
        raise SourceError(f"{self.describe()}: HTTP {status}")


class S3RangeFile(RangeFile):
    def __init__(self, bucket: str, key: str, client=None, **options):
        super().__init__(**options)
        self.bucket = bucket
        self.key = key
        self._client = client

    @property
    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3")
        return self._client

    def describe(self) -> str:
        return f"s3://{self.bucket}/{self.key}"

    def _fetch(self, start: int, stop: int) -> Fetched:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.key, Range=f"bytes={start}-{stop}")
            body = response["Body"].read()
        except botocore.exceptions.ClientError as err:
            code = str(err.response.get("Error", {}).get("Code", ""))
            status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == "InvalidRange":
                return Fetched(b"", 0, None)
            if code in FATAL_S3_CODES:
                raise SourceError(f"{self.describe()}: {code}") from err
            if status in RETRY_STATUSES or code in TRANSIENT_S3_CODES:
                raise TransientError(code or f"HTTP {status}", status) from err
            raise SourceError(f"{self.describe()}: {code or 'S3 request failed'}") from err
        except (
            botocore.exceptions.NoCredentialsError,
            botocore.exceptions.PartialCredentialsError,
            botocore.exceptions.TokenRetrievalError,
        ) as err:
            raise SourceError(f"{self.describe()}: no usable AWS credentials ({err})") from err
        except (
            botocore.exceptions.ConnectionError,
            botocore.exceptions.HTTPClientError,
            botocore.exceptions.IncompleteReadError,
        ) as err:
            raise TransientError(type(err).__name__) from err
        return Fetched(
            body, parse_content_range(response.get("ContentRange")), clean_etag(response.get("ETag"))
        )


class LocalRangeFile(RangeFile):
    def __init__(self, path: str | Path, **options):
        super().__init__(**options)
        self.path = Path(path)
        if not self.path.is_file():
            raise SourceError(f"{self.path}: no such file")
        self._local_size = self.path.stat().st_size

    def describe(self) -> str:
        return str(self.path)

    def _fetch(self, start: int, stop: int) -> Fetched:
        with open(self.path, "rb") as handle:
            handle.seek(start)
            return Fetched(handle.read(stop - start + 1), self._local_size, None)


def open_source(source: str, *, s3_client=None, opener: Opener | None = None, **options) -> RangeFile:
    parts = urlsplit(source)
    if parts.scheme == "s3":
        bucket, key = parts.netloc, parts.path.lstrip("/")
        if not bucket or not key:
            raise SourceError(f"{redact_url(source)}: an S3 source needs a bucket and a key")
        return S3RangeFile(bucket, key, client=s3_client, **options)
    if parts.scheme in ("http", "https"):
        return HttpRangeFile(source, opener=opener, **options)
    if parts.scheme in ("", "file"):
        return LocalRangeFile(parts.path if parts.scheme == "file" else source, **options)
    raise SourceError(f"{redact_url(source)}: unsupported source scheme {parts.scheme!r}")
