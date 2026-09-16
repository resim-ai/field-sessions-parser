from __future__ import annotations

import base64
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .logs import FORMATS, epoch_ns, relative_key

EVENT_PREFIX = "resim:event:"
TOPICS = {
    "session_inventory": {
        "schema": {
            "start_ns": "string",
            "end_ns": "string",
            "files": "string[]",
            "sizes_bytes": "string[]",
            "formats": "string[]",
        }
    },
    "session_events": {
        "event": True,
        "schema": {
            "name": "string",
            "status": "status",
            "description": "string",
            "tags": "string[]",
            "recording_key": "string",
        },
    },
}


def event_tag(recording_key: str, timestamp_ns: int) -> str:
    key = relative_key(recording_key)
    if type(timestamp_ns) is not int or epoch_ns(timestamp_ns) is None:
        raise ValueError("event timestamp must be integer epoch nanoseconds")
    payload = json.dumps(
        {"recording_key": key, "timestamp_ns": str(timestamp_ns)},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return EVENT_PREFIX + base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def event_metadata(tags: Iterable[str]) -> dict[str, str]:
    values = [tag for tag in tags if tag.startswith(EVENT_PREFIX)]
    if len(values) != 1:
        raise ValueError("event must carry exactly one provenance tag")
    encoded = values[0][len(EVENT_PREFIX) :]
    try:
        raw = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        payload = json.loads(raw)
        if set(payload) != {"recording_key", "timestamp_ns"}:
            raise ValueError("unexpected event provenance fields")
        timestamp = payload["timestamp_ns"]
        if not isinstance(timestamp, str) or not timestamp.isascii() or not timestamp.isdecimal():
            raise ValueError("invalid event timestamp")
        if event_tag(payload["recording_key"], int(timestamp)) != values[0]:
            raise ValueError("noncanonical event provenance")
        return payload
    except (TypeError, KeyError, UnicodeError, ValueError) as error:
        raise ValueError("invalid event provenance") from error


def emit_event(
    emitter: Any,
    *,
    recording_key: str,
    timestamp_ns: int,
    name: str,
    status: str = "NO_STATUS_REPORTED",
    description: str = "",
    tags: Iterable[str] = (),
) -> None:
    tags = list(tags)
    if any(not isinstance(tag, str) or tag.startswith(EVENT_PREFIX) for tag in tags):
        raise ValueError("customer tags cannot occupy the event provenance namespace")
    metadata = event_tag(recording_key, timestamp_ns)
    emitter.emit_event(
        "session_events",
        {
            "name": name,
            "status": status,
            "description": description,
            "tags": [*tags, metadata],
            "recording_key": recording_key,
        },
        timestamp=timestamp_ns,
    )


def emit_session(emitter: Any, folder: str | Path, *, start_ns: int, end_ns: int) -> None:
    if type(start_ns) is not int or type(end_ns) is not int or start_ns < 0 or end_ns < start_ns:
        raise ValueError("session bounds must be ordered integer epoch nanoseconds")
    root = Path(folder).resolve(strict=True)
    files = sorted(path for path in root.rglob("*") if path.is_file())
    for path in files:
        if not path.resolve().is_relative_to(root):
            raise ValueError("recording symlink escapes the session folder")
    emitter.emit(
        "session_inventory",
        {
            "start_ns": str(start_ns),
            "end_ns": str(end_ns),
            "files": [relative_key(path.relative_to(root).as_posix()) for path in files],
            "sizes_bytes": [str(path.stat().st_size) for path in files],
            "formats": [FORMATS.get(path.suffix.lower(), "unsupported") for path in files],
        },
        timestamp=start_ns,
    )
