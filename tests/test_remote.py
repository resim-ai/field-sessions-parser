from __future__ import annotations

import http.client
import io
import os
import urllib.error
from unittest.mock import MagicMock

import botocore.exceptions
import pytest
from range_server import FakeOpener, RangeServer
from stub_s3 import StubS3Client

from field_sessions_parser import remote
from field_sessions_parser.remote import (
    HttpRangeFile,
    LocalRangeFile,
    S3RangeFile,
    SourceError,
    open_source,
    parse_content_range,
    redact_url,
)

KIB = 1024
DATA = bytes(range(256)) * (4 * KIB)  # 1 MiB with a recognisable pattern
URL = "https://bucket.s3.amazonaws.com/prefix/drive.mcap?X-Amz-Signature=deadbeef&X-Amz-Expires=3600"


def http_file(opener: FakeOpener, **options) -> HttpRangeFile:
    options.setdefault("block_size", 64 * KIB)
    options.setdefault("sleep", lambda _s: None)
    return HttpRangeFile(URL, opener=opener, **options)


def test_size_probe_reads_the_head_once():
    opener = FakeOpener(DATA)
    stream = http_file(opener)
    assert stream.size == len(DATA)
    assert stream.etag == "fixture-etag"
    assert stream.requests == 1
    assert opener.calls == [(0, remote.HEAD_PROBE_BYTES - 1)]
    assert stream.bytes_fetched == remote.HEAD_PROBE_BYTES


def test_head_bytes_are_served_from_the_probe():
    opener = FakeOpener(DATA)
    stream = http_file(opener)
    stream.seek(0)
    assert stream.read(8) == DATA[:8]
    assert stream.requests == 1


def test_footer_read_fetches_only_the_trailing_block():
    opener = FakeOpener(DATA)
    stream = http_file(opener)
    stream.seek(-37, io.SEEK_END)
    assert stream.read(37) == DATA[-37:]
    assert stream.requests == 2
    start, end = opener.calls[-1]
    assert start == len(DATA) - 64 * KIB
    assert end == len(DATA) - 1


def test_span_across_blocks_is_one_request():
    opener = FakeOpener(DATA)
    stream = http_file(opener)
    _ = stream.size
    stream.seek(100 * KIB)
    assert stream.read(200 * KIB) == DATA[100 * KIB : 300 * KIB]
    assert stream.requests == 2
    assert opener.calls[-1] == (64 * KIB, 5 * 64 * KIB - 1)


def test_blocks_per_request_are_capped():
    opener = FakeOpener(DATA)
    stream = http_file(opener, max_blocks_per_request=2)
    _ = stream.size
    stream.seek(0)
    stream.read(5 * 64 * KIB)
    assert stream.requests == 1 + 3


def test_lru_is_bounded():
    opener = FakeOpener(DATA)
    stream = http_file(opener, max_cached_blocks=4)
    for block in range(8):
        stream.seek(block * 64 * KIB)
        stream.read(16)
    assert stream.cached_blocks == 4
    stream.seek(0)
    assert stream.read(16) == DATA[:16]


def test_read_spanning_more_blocks_than_the_cache_holds():
    opener = FakeOpener(DATA)
    stream = http_file(opener, max_cached_blocks=4, max_blocks_per_request=3)
    _ = stream.size
    stream.seek(2 * 64 * KIB)
    assert stream.read(10 * 64 * KIB) == DATA[2 * 64 * KIB : 12 * 64 * KIB]
    assert stream.requests == 1 + 4
    assert stream.cached_blocks == 4
    stream.seek(11 * 64 * KIB)
    assert stream.read(16) == DATA[11 * 64 * KIB : 11 * 64 * KIB + 16]
    assert stream.requests == 1 + 4


def test_range_ignoring_server_is_rejected():
    opener = FakeOpener(DATA, ignore_range=True)
    stream = http_file(opener)
    with pytest.raises(SourceError, match="ignored Range"):
        _ = stream.size
    assert stream.requests == 1
    assert stream.bytes_fetched == 0


@pytest.mark.parametrize("data", [b"", b"x", b"short object"])
def test_probe_accepts_empty_and_smaller_than_probe_objects(data):
    stream = http_file(FakeOpener(data))
    assert stream.size == len(data)
    assert stream.read() == data
    stream.seek(-min(1, len(data)), io.SEEK_END)
    assert stream.read() == data[-1:]
    assert stream.requests == 1


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Content-Range": "bytes 100-4195/100000"},
        {"Content-Range": "bytes 0-4094/100000"},
        {"Content-Range": "bytes 0-4096/100000"},
        {"Content-Range": "bytes 0-4095/*"},
        {"Content-Range": "bytes 0-4095/0"},
        {"Content-Range": "bytes -1-4095/100000"},
        {"Content-Range": "bytes 0-4095/100000", "Content-Length": "4095"},
        {"Content-Range": "bytes 0-4095/100000", "Content-Length": "invalid"},
        {"Content-Range": "bytes 0-4095/100000", "Content-Encoding": "gzip"},
    ],
)
def test_bad_range_metadata_fails_before_accepting_probe(headers):
    stream = http_file(lambda *_: (206, headers, DATA[:4096]))
    with pytest.raises(SourceError, match="HTTP 206"):
        stream.read(4)
    assert stream.requests == 1
    assert stream.tell() == stream.bytes_fetched == stream.cached_blocks == 0
    assert stream._size is None
    assert stream._head == b""


@pytest.mark.parametrize("content_range", [None, "bytes */100000", "bytes */unknown"])
def test_invalid_unsatisfied_probe_is_not_an_empty_object(content_range):
    headers = {} if content_range is None else {"Content-Range": content_range}
    stream = http_file(lambda *_: (416, headers, b""))
    with pytest.raises(SourceError, match="invalid HTTP 416"):
        _ = stream.size
    assert stream._size is None


def test_short_block_body_is_discarded_then_retried_at_the_same_offsets():
    source = FakeOpener(DATA)
    sleeps = []

    def opener(url, headers):
        status, metadata, body = source(url, headers)
        return status, metadata, body[:7] if len(source.calls) == 2 else body

    stream = http_file(opener, sleep=sleeps.append)
    stream.seek(70 * KIB)
    assert stream.read(32) == DATA[70 * KIB : 70 * KIB + 32]
    assert source.calls[1:] == [(64 * KIB, 128 * KIB - 1)] * 2
    assert sleeps == [0.5]
    assert stream.bytes_fetched == remote.HEAD_PROBE_BYTES + 64 * KIB


@pytest.mark.parametrize("change", ["size", "etag", "missing_etag"])
def test_changed_object_cannot_supply_cached_blocks(change):
    source = FakeOpener(DATA)

    def opener(url, headers):
        status, metadata, body = source(url, headers)
        if len(source.calls) > 1:
            if change == "size":
                metadata["Content-Range"] = metadata["Content-Range"].replace(
                    f"/{len(DATA)}", f"/{len(DATA) + 1}"
                )
            elif change == "etag":
                metadata["ETag"] = '"replacement"'
            else:
                del metadata["ETag"]
        return status, metadata, body

    stream = http_file(opener)
    assert stream.size == len(DATA)
    stream.seek(70 * KIB)
    with pytest.raises(SourceError, match="changed during range reads"):
        stream.read(32)
    assert stream.requests == 2
    assert stream.cached_blocks == 0
    assert stream.tell() == 70 * KIB


def test_server_without_etags_remains_supported():
    source = FakeOpener(DATA)

    def opener(url, headers):
        status, metadata, body = source(url, headers)
        del metadata["ETag"]
        return status, metadata, body

    stream = http_file(opener)
    assert stream.etag is None
    stream.seek(-9, io.SEEK_END)
    assert stream.read() == DATA[-9:]


def test_real_urllib_rejects_ignored_range_without_reading_body(monkeypatch):
    response = MagicMock(status=200, headers={"Content-Length": "5000000000"})
    response.__enter__.return_value = response
    monkeypatch.setattr(remote.urllib.request, "urlopen", lambda *_args, **_kwargs: response)
    stream = HttpRangeFile(URL)
    with pytest.raises(SourceError, match="ignored Range"):
        _ = stream.size
    response.read.assert_not_called()
    response.__exit__.assert_called_once()


def test_real_urllib_bounds_body_read_and_rejects_excess(monkeypatch):
    response = MagicMock(status=206, headers={"Content-Range": "bytes 0-4095/100000"})
    response.__enter__.return_value = response
    response.read.return_value = b"x" * 4097
    monkeypatch.setattr(remote.urllib.request, "urlopen", lambda *_args, **_kwargs: response)
    stream = HttpRangeFile(URL)
    with pytest.raises(SourceError, match="exceeds the requested range"):
        _ = stream.size
    response.read.assert_called_once_with(4097)
    assert stream.bytes_fetched == 0


def test_real_urllib_rejects_invalid_range_before_reading_body(monkeypatch):
    response = MagicMock(status=206, headers={"Content-Range": "bytes 100-4195/100000"})
    response.__enter__.return_value = response
    monkeypatch.setattr(remote.urllib.request, "urlopen", lambda *_args, **_kwargs: response)
    stream = HttpRangeFile(URL)
    with pytest.raises(SourceError, match="does not match"):
        _ = stream.size
    response.read.assert_not_called()


def test_real_urllib_incomplete_read_retries_and_discards_partial_bytes(monkeypatch):
    response = MagicMock(status=206, headers={"Content-Range": "bytes 0-4095/100000"})
    response.__enter__.return_value = response
    response.read.side_effect = [http.client.IncompleteRead(b"corrupt", 4090), DATA[:4096]]
    requests = []

    def urlopen(request, **_kwargs):
        requests.append((request.full_url, request.get_header("Range")))
        return response

    monkeypatch.setattr(remote.urllib.request, "urlopen", urlopen)
    sleeps = []
    stream = HttpRangeFile(URL, sleep=sleeps.append)
    assert stream.read(8) == DATA[:8]
    assert requests == [(URL, "bytes=0-4095")] * 2
    assert sleeps == [0.5]
    assert stream.bytes_fetched == 4096


def test_real_urllib_transport_error_exhaustion_hides_signed_url(monkeypatch):
    def urlopen(*_args, **_kwargs):
        raise urllib.error.URLError(f"connection failed: {URL}")

    monkeypatch.setattr(remote.urllib.request, "urlopen", urlopen)
    stream = HttpRangeFile(URL, max_attempts=2, sleep=lambda _: None)
    with pytest.raises(SourceError, match="giving up after 2 attempts") as raised:
        _ = stream.size
    assert "deadbeef" not in str(raised.value)
    assert "X-Amz" not in str(raised.value)
    assert stream.requests == 2


@pytest.mark.parametrize("truncations, succeeds", [(1, True), (10, False)])
def test_real_http_truncated_response_has_bounded_retries(truncations, succeeds):
    sleeps = []
    with RangeServer(DATA, truncate_responses=truncations) as server:
        stream = HttpRangeFile(server.url, max_attempts=3, sleep=sleeps.append)
        if succeeds:
            assert stream.read(8) == DATA[:8]
            assert stream.requests == 2
            assert sleeps == [0.5]
        else:
            with pytest.raises(SourceError, match="giving up after 3 attempts"):
                stream.read(8)
            assert stream.requests == 3
            assert stream.tell() == stream.bytes_fetched == stream.cached_blocks == 0
            assert stream._size is None
            assert stream._head == b""
            assert sleeps == [0.5, 1.0]
        assert server.requests == ["bytes=0-4095"] * stream.requests


def test_forbidden_mentions_expiry_and_hides_the_signature():
    opener = FakeOpener(DATA, fail_status=403, fail_times=None)
    stream = http_file(opener)
    with pytest.raises(SourceError) as excinfo:
        _ = stream.size
    message = str(excinfo.value)
    assert "expired" in message
    assert "Signature" not in message and "deadbeef" not in message
    assert "prefix/drive.mcap" in message
    assert stream.requests == 1


def test_not_found_is_an_error():
    opener = FakeOpener(DATA, fail_status=404, fail_times=None)
    stream = http_file(opener)
    with pytest.raises(SourceError, match="404"):
        _ = stream.size


def test_transient_status_is_retried_with_backoff():
    sleeps: list[float] = []
    opener = FakeOpener(DATA, fail_status=503, fail_times=2)
    stream = http_file(opener, sleep=sleeps.append)
    assert stream.size == len(DATA)
    assert stream.requests == 3
    assert sleeps == [0.5, 1.0]


def test_transient_status_gives_up_after_max_attempts():
    sleeps: list[float] = []
    opener = FakeOpener(DATA, fail_status=502, fail_times=None)
    stream = http_file(opener, sleep=sleeps.append, max_attempts=3)
    with pytest.raises(SourceError, match="giving up after 3 attempts"):
        _ = stream.size
    assert len(sleeps) == 2


def test_real_http_server_via_urllib():
    with RangeServer(DATA) as server:
        stream = HttpRangeFile(f"{server.url}/drive.mcap", block_size=64 * KIB)
        assert stream.size == len(DATA)
        assert stream.etag == "server-etag"
        stream.seek(300 * KIB + 5)
        assert stream.read(100) == DATA[300 * KIB + 5 : 300 * KIB + 105]
        assert server.requests[0] == f"bytes=0-{remote.HEAD_PROBE_BYTES - 1}"


def test_s3_reads_through_get_object():
    client = StubS3Client({("bucket", "prefix/drive.mcap"): DATA})
    stream = S3RangeFile("bucket", "prefix/drive.mcap", client=client, block_size=64 * KIB)
    assert stream.size == len(DATA)
    assert stream.etag == "stub-etag"
    stream.seek(65 * KIB)
    assert stream.read(2 * KIB) == DATA[65 * KIB : 67 * KIB]
    assert client.calls[0] == ("bucket", "prefix/drive.mcap", f"bytes=0-{remote.HEAD_PROBE_BYTES - 1}")
    assert client.calls[1] == ("bucket", "prefix/drive.mcap", f"bytes={64 * KIB}-{128 * KIB - 1}")


@pytest.mark.parametrize("code", ["AccessDenied", "NoSuchKey", "NoSuchBucket", "InvalidObjectState"])
def test_s3_fatal_codes_raise_at_once(code):
    client = StubS3Client({("bucket", "k"): DATA})
    client.fail_next(code, 403)
    stream = S3RangeFile("bucket", "k", client=client, sleep=lambda _s: None)
    with pytest.raises(SourceError, match=code):
        _ = stream.size
    assert len(client.calls) == 1


def test_s3_throttling_is_retried():
    sleeps: list[float] = []
    client = StubS3Client({("bucket", "k"): DATA})
    client.fail_next("SlowDown", 503)
    client.fail_next("InternalError", 500)
    stream = S3RangeFile("bucket", "k", client=client, block_size=64 * KIB, sleep=sleeps.append)
    assert stream.size == len(DATA)
    assert len(client.calls) == 3
    assert sleeps == [0.5, 1.0]


def test_s3_incomplete_body_read_is_retried():
    sleeps: list[float] = []
    client = StubS3Client({("bucket", "k"): DATA})
    client.fail_body_next(botocore.exceptions.IncompleteReadError(actual_bytes=10, expected_bytes=4096))
    stream = S3RangeFile("bucket", "k", client=client, block_size=64 * KIB, sleep=sleeps.append)
    assert stream.size == len(DATA)
    assert len(client.calls) == 2
    assert sleeps == [0.5]


@pytest.mark.parametrize(
    "error",
    [
        botocore.exceptions.NoCredentialsError(),
        botocore.exceptions.PartialCredentialsError(provider="env", cred_var="AWS_SECRET_ACCESS_KEY"),
        botocore.exceptions.TokenRetrievalError(provider="sso", error_msg="token expired"),
    ],
)
def test_s3_credential_errors_fail_at_once(error):
    client = StubS3Client({("bucket", "k"): DATA})
    client.fail_next_with(error)
    stream = S3RangeFile("bucket", "k", client=client, sleep=lambda _s: None)
    with pytest.raises(SourceError, match="credentials"):
        _ = stream.size
    assert len(client.calls) == 1


def test_s3_missing_key_is_an_error():
    client = StubS3Client({})
    stream = S3RangeFile("bucket", "missing", client=client)
    with pytest.raises(SourceError, match="NoSuchKey"):
        _ = stream.size


def test_open_source_dispatches_by_scheme(tmp_path):
    client = StubS3Client({("bucket", "k"): DATA})
    assert isinstance(open_source("s3://bucket/k", s3_client=client), S3RangeFile)
    assert isinstance(open_source("https://example.test/k", opener=FakeOpener(DATA)), HttpRangeFile)
    path = tmp_path / "local.bin"
    path.write_bytes(DATA)
    local = open_source(str(path), block_size=64 * KIB)
    assert isinstance(local, LocalRangeFile)
    assert local.size == len(DATA)
    assert local.etag is None
    local.seek(-8, os.SEEK_END)
    assert local.read(8) == DATA[-8:]
    assert local.requests == 2


def test_open_source_rejects_bad_sources(tmp_path):
    with pytest.raises(SourceError):
        open_source("s3://bucket")
    with pytest.raises(SourceError):
        open_source(str(tmp_path / "missing.mcap"))
    with pytest.raises(SourceError):
        open_source("ftp://example.test/k")


def test_helpers():
    assert parse_content_range("bytes 0-4095/175000000") == 175000000
    assert parse_content_range("bytes */12") == 12
    assert parse_content_range(None) is None
    assert redact_url(URL) == "https://bucket.s3.amazonaws.com/prefix/drive.mcap"
    assert redact_url("https://user:password@example.test:443/path?signature=secret#fragment") == (
        "https://example.test:443/path"
    )
