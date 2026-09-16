"""A stub boto3 S3 client: get_object with Body and ContentRange, plus injected errors."""

from __future__ import annotations

import io

from botocore.exceptions import ClientError
from range_server import parse_range


class FailingBody:
    def __init__(self, error: Exception):
        self.error = error

    def read(self, amt=None):
        raise self.error


class StubS3Client:
    def __init__(self, objects: dict[tuple[str, str], bytes], *, etag: str = '"stub-etag"'):
        self.objects = objects
        self.etag = etag
        self.calls: list[tuple[str, str, str]] = []
        self.errors: list[Exception] = []
        self.body_errors: list[Exception] = []

    def fail_next(self, code: str, status: int) -> None:
        self.errors.append(self._error(code, status))

    def fail_next_with(self, error: Exception) -> None:
        self.errors.append(error)

    def fail_body_next(self, error: Exception) -> None:
        self.body_errors.append(error)

    def get_object(self, Bucket: str, Key: str, Range: str):
        self.calls.append((Bucket, Key, Range))
        if self.errors:
            raise self.errors.pop(0)
        data = self.objects.get((Bucket, Key))
        if data is None:
            raise self._error("NoSuchKey", 404)
        wanted = parse_range(Range, len(data))
        if wanted is None or wanted[0] >= len(data):
            raise self._error("InvalidRange", 416)
        start, end = wanted
        body = data[start : end + 1]
        return {
            "Body": FailingBody(self.body_errors.pop(0)) if self.body_errors else io.BytesIO(body),
            "ContentRange": f"bytes {start}-{end}/{len(data)}",
            "ContentLength": len(body),
            "ETag": self.etag,
        }

    @staticmethod
    def _error(code: str, status: int) -> ClientError:
        return ClientError(
            {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
            "GetObject",
        )
