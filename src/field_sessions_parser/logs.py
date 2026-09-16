from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, is_dataclass
from dataclasses import fields as dataclass_fields
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .reader import UNDECODABLE, LogError, LogReader
from .remote import open_source

FORMATS = {".mcap": "mcap"}


@dataclass(frozen=True)
class Record:
    topic: str
    timestamp_ns: int | None
    data: Any
    recording_key: str


def relative_key(key: str) -> str:
    if not isinstance(key, str) or not key or key.startswith("/") or "\\" in key:
        raise ValueError("recording key must be relative to the session folder")
    if any(part in {"", ".", ".."} for part in key.split("/")) or any(ord(c) < 32 for c in key):
        raise ValueError("recording key contains an unsafe path component")
    return key


def epoch_ns(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError("timestamps must be integers or decimal strings, not floats")
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number < 0 or number != number.to_integral_value():
            raise ValueError("timestamp must resolve to nonnegative integral nanoseconds")
        return int(number)
    except InvalidOperation as error:
        raise ValueError("invalid timestamp") from error


class Recording:
    def __init__(
        self,
        source: str | Path,
        *,
        recording_key: str | None = None,
        s3_client=None,
    ):
        self.source = str(source)
        self.key = relative_key(recording_key or Path(urlsplit(self.source).path).name)
        self.format = FORMATS.get(Path(urlsplit(self.source).path).suffix.lower())
        if self.format is None:
            raise ValueError(f"unsupported recording format: {self.key}")
        self.s3_client = s3_client

    def iter_messages(self, topics: set[str] | None = None) -> Iterator[Record]:
        with open_source(self.source, s3_client=self.s3_client) as stream:
            reader = LogReader(stream).open()
            for channel, message in reader.iter_messages(topics):
                data = reader.decode(channel, message)
                if data is UNDECODABLE:
                    raise LogError(f"cannot decode {self.key}:{channel.topic}")
                yield Record(channel.topic, message.log_time, data, self.key)
            if reader.decode_failures:
                raise LogError(f"incomplete recording {self.key}: {reader.decode_failures}")

    def inspect(self) -> dict[str, Any]:
        counts: Counter[str] = Counter()
        missing = 0
        start = end = None
        fields: dict[str, set[str]] = {}
        for record in self.iter_messages():
            counts[record.topic] += 1
            fields.setdefault(record.topic, set()).update(_field_paths(record.data))
            if record.timestamp_ns is None:
                missing += 1
            else:
                start = record.timestamp_ns if start is None else min(start, record.timestamp_ns)
                end = record.timestamp_ns if end is None else max(end, record.timestamp_ns)
        return {
            "recording_key": self.key,
            "format": self.format,
            "message_count": sum(counts.values()),
            "topics": dict(counts),
            "fields": {k: sorted(v) for k, v in fields.items()},
            "start_ns": start,
            "end_ns": end,
            "unclocked_messages": missing,
            "complete": True,
        }


def _field_paths(value: Any, prefix: str = "", depth: int = 0) -> Iterator[str]:
    if depth >= 8:
        return
    if isinstance(value, dict):
        items = value.items()
    elif is_dataclass(value) and not isinstance(value, type):
        items = (
            (field.name, getattr(value, field.name))
            for field in dataclass_fields(value)
            if not field.name.startswith("__")
        )
    else:
        return
    for name, child in items:
        path = f"{prefix}.{name}" if prefix else str(name)
        yield path
        yield from _field_paths(child, path, depth + 1)


def open(source: str | Path, **options) -> Recording:
    return Recording(source, **options)


def iter_messages(source: str | Path, **options) -> Iterator[Record]:
    return open(source, **options).iter_messages()


def inspect(source: str | Path, **options) -> dict[str, Any]:
    return open(source, **options).inspect()
