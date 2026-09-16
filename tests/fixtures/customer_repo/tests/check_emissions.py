"""Validate the real job's SDK output and fail-closed entrypoint behavior."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from collections import Counter
from importlib.metadata import version
from pathlib import Path

import yaml
from resim.sdk.metrics.emissions import Emitter

from field_sessions_parser.emissions import event_metadata

START = 1789420800000000001
KEYS = ["legacy.mcap", "nested/prefixed.mcap"]
INPUT = Path("/tmp/resim/inputs/experience")


def check_output(config_path: Path, output: Path) -> None:
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    counts = Counter(row["$metadata"]["topic"] for row in rows)
    assert counts == {
        "session_inventory": 1,
        "recording_topics": 6,
        "burro_state": 10,
        "burro_gps": 8,
        "session_events": 10,
    }, counts
    config = yaml.safe_load(config_path.read_text())
    with tempfile.TemporaryDirectory() as validation_dir:
        emitter = Emitter(
            config_path=str(config_path), output_path=str(Path(validation_dir) / "validated.jsonl")
        )
        try:
            for row in rows:
                metadata, data = row["$metadata"], row["$data"]
                topic, stamp = metadata["topic"], metadata["timestamp"]
                assert type(stamp) is int and START <= stamp <= START + 9
                assert set(data) == set(config["topics"][topic]["schema"])
                if topic == "session_events":
                    assert metadata.get("event") is True
                    emitter.emit_event(topic, data, timestamp=stamp)
                else:
                    assert not metadata.get("event", False)
                    emitter.emit(topic, data, timestamp=stamp)
        finally:
            emitter.close()
    inventory = next(row for row in rows if row["$metadata"]["topic"] == "session_inventory")
    assert inventory["$metadata"]["timestamp"] == START
    assert inventory["$data"] == {
        "start_ns": str(START),
        "end_ns": str(START + 9),
        "files": KEYS,
        "formats": ["mcap", "mcap"],
        "sizes_bytes": [str((INPUT / key).stat().st_size) for key in KEYS],
    }
    for key in KEYS:

        def selected(topic, recording_key=key):
            return [
                row
                for row in rows
                if row["$metadata"]["topic"] == topic and row["$data"]["recording_key"] == recording_key
            ]

        states = selected("burro_state")
        assert [
            (
                r["$metadata"]["timestamp"],
                r["$data"]["state"],
                r["$data"]["obstacle_stop"],
                r["$data"]["stop_observation"],
            )
            for r in states
        ] == [
            (START, 0, False, False),
            (START + 1, 6, True, True),
            (START + 2, 4, True, False),
            (START + 3, 0, False, False),
            (START + 4, 4, True, True),
        ]
        gps = selected("burro_gps")
        assert [
            (
                r["$metadata"]["timestamp"],
                r["$data"]["fix_status"],
                r["$data"]["has_previous"],
                r["$data"]["fix_changed"],
            )
            for r in gps
        ] == [
            (START + 5, "Fixed", False, False),
            (START + 6, "Fixed", True, False),
            (START + 7, "Float", True, True),
            (START + 8, "Fixed", True, True),
        ]
        assert {
            r["$data"]["topic_name"]: r["$data"]["message_count"] for r in selected("recording_topics")
        } == {
            "/unicycle/unicycle_node/state": 5,
            "/gps_status": 4,
            "/other": 1,
        }
        events = selected("session_events")
        assert Counter((r["$data"]["name"], r["$metadata"]["timestamp"]) for r in events) == Counter(
            [
                ("Obstacle stop observed", START + 1),
                ("Obstacle stop observed", START + 4),
                ("GPS fix changed", START + 7),
                ("GPS fix changed", START + 8),
                ("Recording started", START),
            ]
        )
        for row in events:
            assert row["$data"]["status"] == "NO_STATUS_REPORTED"
            assert event_metadata(row["$data"]["tags"]) == {
                "recording_key": key,
                "timestamp_ns": str(row["$metadata"]["timestamp"]),
            }
    print(
        "EMISSIONS PASS: 35 rows, five topics, baked SDK schemas, exact ns, "
        "complete inventory, per-file state/GPS transitions and 10 events",
        flush=True,
    )


def main() -> None:
    config_path, output = map(Path, sys.argv[1:])
    check_output(config_path, output)
    for filename, contents, reason in [
        ("z-bad.mcap", "corrupt MCAP\n", "too small to be an MCAP"),
        ("z-unsupported.xyz", "synthetic\n", "unsupported input file"),
    ]:
        bad = INPUT / filename
        bad.write_text(contents)
        output.write_text("stale output must be removed on failure\n")
        try:
            result = subprocess.run(
                [sys.executable, "/app/job.py"], capture_output=True, text=True, check=False
            )
            assert result.returncode != 0, filename
            assert reason in result.stderr, result.stderr
            assert not output.exists(), "failure retained successful/partial output"
            assert not output.with_name(output.name + ".pending").exists()
        finally:
            bad.unlink()
        print(f"FAILURE PASS: {filename} rejected with no retained output", flush=True)
    subprocess.run([sys.executable, "/app/job.py"], check=True)
    check_output(config_path, output)
    print(
        f"SDK CHECK PASS: resim-open-core={version('resim-open-core')}; actual /app/job.py; no skipped tests",
        flush=True,
    )


if __name__ == "__main__":
    main()
