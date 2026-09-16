from __future__ import annotations

import base64
import json

import pytest

from field_sessions_parser.emissions import TOPICS, emit_event, emit_session, event_metadata, event_tag

STAMP = 1789420800000000001


def test_provenance_preserves_exact_time_and_unicode_key():
    tag = event_tag("nested/走行.mcap", STAMP)
    assert event_metadata(["obstacle", tag]) == {
        "recording_key": "nested/走行.mcap",
        "timestamp_ns": str(STAMP),
    }
    assert "=" not in tag


@pytest.mark.parametrize(
    "key", ["", "/file.mcap", "../file.mcap", "nested/../file.mcap", "a//b", "a\\b", "a\x00b"]
)
def test_unsafe_recording_key_rejected(key):
    with pytest.raises(ValueError):
        event_tag(key, STAMP)


def test_duplicate_malformed_and_noncanonical_provenance_rejected():
    tag = event_tag("drive.mcap", STAMP)
    bad_payload = (
        base64.urlsafe_b64encode(b'{"recording_key":"drive.mcap","timestamp_ns":"01"}').decode().rstrip("=")
    )
    for tags in [[], [tag, tag], ["resim:event:not-json"], [tag + "="], ["resim:event:" + bad_payload]]:
        with pytest.raises(ValueError):
            event_metadata(tags)


def test_customer_tag_namespace_collision_precedes_emission():
    with pytest.raises(ValueError, match="namespace"):
        emit_event(None, recording_key="drive.mcap", timestamp_ns=STAMP, name="stop", tags=["resim:event:x"])


@pytest.mark.sdk
def test_real_sdk_output_schema_time_inventory_and_provenance(tmp_path):
    import yaml
    from resim.sdk.metrics.emissions import Emitter

    folder = tmp_path / "session"
    folder.mkdir()
    (folder / "first.mcap").write_bytes(b"first")
    (folder / "second.mcap").write_bytes(b"second")
    config = tmp_path / "config.resim.yml"
    config.write_text(yaml.safe_dump({"version": 1, "topics": TOPICS}))
    output = tmp_path / "emissions.resim.jsonl"
    emitter = Emitter(config_path=str(config), output_path=str(output))
    emit_session(emitter, folder, start_ns=STAMP, end_ns=STAMP + 1)
    for key in ["first.mcap", "second.mcap"]:
        emit_event(
            emitter, recording_key=key, timestamp_ns=STAMP, name="stop", status="FAIL_WARN", tags=["stop"]
        )
    emitter.close()
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert len(rows) == 3
    assert all(row["$metadata"]["timestamp"] == STAMP for row in rows)
    assert rows[0]["$data"] == {
        "start_ns": str(STAMP),
        "end_ns": str(STAMP + 1),
        "files": ["first.mcap", "second.mcap"],
        "sizes_bytes": ["5", "6"],
        "formats": ["mcap", "mcap"],
    }
    for row, key in zip(rows[1:], ["first.mcap", "second.mcap"], strict=True):
        assert row["$metadata"]["event"] is True
        assert event_metadata(row["$data"]["tags"]) == {"recording_key": key, "timestamp_ns": str(STAMP)}
