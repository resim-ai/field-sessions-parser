from __future__ import annotations

import importlib.util
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from conftest import NS, T0
from mcap.writer import Writer
from rosbags.typesys import Stores, get_types_from_msg, get_typestore

from field_sessions_parser.emissions import TOPICS, event_metadata
from field_sessions_parser.reader import NotMcapError

REPO = Path(__file__).parent / "fixtures" / "customer_repo"


class CapturingEmitter:
    def __init__(self):
        self.rows = []

    def emit(self, topic, data, *, timestamp):
        self.rows.append((topic, timestamp, data))

    emit_event = emit


@pytest.fixture
def job(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO))
    spec = importlib.util.spec_from_file_location("customer_job", REPO / "job.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_inventory_counts_all_recordings_without_robot_analysis(job, tmp_path):
    folder = tmp_path / "drive"
    folder.mkdir()
    (folder / "nested").mkdir()
    for key in ["first.mcap", "nested/second.mcap"]:
        write_burro_state_mcap(folder / key, "uint16 STATE_OBSTACLE_STOP=4")
    emitter = CapturingEmitter()
    job.run(folder, emitter)
    assert {topic for topic, _, _ in emitter.rows} == {
        "session_inventory",
        "recording_topics",
        "session_events",
    }
    assert sum(data["message_count"] for topic, _, data in emitter.rows if topic == "recording_topics") == 12
    events = [(stamp, data) for topic, stamp, data in emitter.rows if topic == "session_events"]
    assert len(events) == 2
    for stamp, data in events:
        assert data["name"] == "Recording started"
        assert event_metadata(data["tags"]) == {
            "recording_key": data["recording_key"],
            "timestamp_ns": str(T0),
        }
        assert stamp == T0
    inventory = next(data for topic, _, data in emitter.rows if topic == "session_inventory")
    assert inventory["files"] == ["first.mcap", "nested/second.mcap"]
    assert inventory["start_ns"] == str(T0)
    assert inventory["end_ns"] == str(T0 + 5)


def test_baked_config_matches_helpers():
    config = yaml.safe_load((REPO / "config.resim.yml").read_text())
    assert {key: config["topics"][key] for key in TOPICS} == TOPICS
    assert set(config["metrics sets"]["Session metrics"]["metrics"]) <= config["metrics"].keys()


@pytest.mark.parametrize("duration_ns", [1, 10_397_585_360])
def test_duration_sql_preserves_epoch_nanoseconds(duration_ns):
    config = yaml.safe_load((REPO / "config.resim.yml").read_text())
    schema = config["topics"]["session_inventory"]["schema"]
    assert schema["start_ns"] == schema["end_ns"] == "string"
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE session_inventory (start_ns TEXT, end_ns TEXT)")
        db.execute("INSERT INTO session_inventory VALUES (?, ?)", (str(T0), str(T0 + duration_ns)))
        value = db.execute(config["metrics"]["Session duration"]["query_string"]).fetchone()[0]
    assert value == pytest.approx(duration_ns / 1_000_000_000, rel=1e-12, abs=0)


def test_empty_recording_folder_fails(job, tmp_path):
    with pytest.raises(ValueError, match="no recordings"):
        job.run(tmp_path, CapturingEmitter())


def test_missing_clock_fails(job, monkeypatch, tmp_path):
    (tmp_path / "recording.mcap").touch()
    record = SimpleNamespace(topic="/sample", timestamp_ns=None, data={})
    monkeypatch.setattr(
        job.logs, "open", lambda *args, **kwargs: SimpleNamespace(iter_messages=lambda: iter([record]))
    )
    with pytest.raises(ValueError, match="missing clock"):
        job.run(tmp_path, CapturingEmitter())


def test_corrupt_mcap_after_valid_recording_is_not_silently_skipped(job, tmp_path):
    write_burro_state_mcap(tmp_path / "a-valid.mcap", "uint16 STATE_OBSTACLE_STOP=4")
    (tmp_path / "z-bad.mcap").write_text("corrupt MCAP\n")
    emitter = CapturingEmitter()
    with pytest.raises(NotMcapError, match="too small to be an MCAP"):
        job.run(tmp_path, emitter)
    assert any(data.get("recording_key") == "a-valid.mcap" for _, _, data in emitter.rows)
    assert not any(topic == "session_inventory" for topic, _, _ in emitter.rows)


def test_unsupported_and_escaping_files_fail_before_emission(job, tmp_path):
    folder = tmp_path / "drive"
    folder.mkdir()
    outside = tmp_path / "outside.log"
    outside.write_text('{"timestamp_ns":1789420800000000001}\n')
    (folder / "escape.log").symlink_to(outside)
    emitter = CapturingEmitter()
    with pytest.raises(ValueError, match="symlink"):
        job.run(folder, emitter)
    assert emitter.rows == []
    (folder / "escape.log").unlink()
    (folder / "unsupported.xyz").touch()
    with pytest.raises(ValueError, match="unsupported"):
        job.run(folder, emitter)
    assert emitter.rows == []


@pytest.mark.parametrize("suffix", [".bag", ".log", ".hdf5", ".parquet"])
def test_non_mcap_files_are_rejected_before_emission(job, tmp_path, suffix):
    (tmp_path / ("recording" + suffix)).write_bytes(b"unsupported recording")
    emitter = CapturingEmitter()
    with pytest.raises(ValueError, match="unsupported input file"):
        job.run(tmp_path, emitter)
    assert emitter.rows == []


@pytest.mark.parametrize("target_kind", ["directory", "missing"])
def test_directory_and_dangling_symlinks_are_not_silently_omitted(job, tmp_path, target_kind):
    folder = tmp_path / "drive"
    folder.mkdir()
    target = tmp_path / "target"
    if target_kind == "directory":
        target.mkdir()
    (folder / "link").symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        job.run(folder, CapturingEmitter())


@pytest.mark.parametrize("failure_stage", ["init", "close"])
def test_output_is_removed_when_sdk_construction_or_close_fails(job, tmp_path, failure_stage):
    folder = tmp_path / "drive"
    folder.mkdir()
    write_burro_state_mcap(folder / "records.mcap", "uint16 STATE_OBSTACLE_STOP=4")
    output = tmp_path / "emissions.resim.jsonl"
    output.write_text("stale previous output")

    class FailingEmitter(CapturingEmitter):
        def __init__(self, *, config_path, output_path):
            super().__init__()
            Path(output_path).write_text("partial output")
            if failure_stage == "init":
                raise RuntimeError("initialization failed")

        def close(self):
            if failure_stage == "close":
                raise RuntimeError("flush failed")

    with pytest.raises(RuntimeError):
        job.write_output(folder, output, REPO / "config.resim.yml", "inventory", FailingEmitter)
    assert not output.exists()
    assert not output.with_name(output.name + ".pending").exists()


@pytest.mark.sdk
def test_real_inventory_entrypoint_and_emitter(tmp_path):
    folder = tmp_path / "drive"
    folder.mkdir()
    write_burro_state_mcap(folder / "drive.mcap", "uint16 STATE_OBSTACLE_STOP=4")
    output = tmp_path / "outputs" / "emissions.resim.jsonl"
    command = [sys.executable, str(REPO / "job.py"), "--input", str(folder), "--output", str(output)]
    subprocess.run(command, check=True)
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert len([row for row in rows if row["$metadata"].get("event")]) == 1
    assert all(type(row["$metadata"]["timestamp"]) is int for row in rows)
    (folder / "z-bad.mcap").write_bytes(b"corrupt MCAP")
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert not output.exists()
    assert not output.with_name(output.name + ".pending").exists()


def test_burro_combined_bits_and_gps_changes_keep_recording_boundaries(job, monkeypatch, tmp_path):
    def record(topic, stamp, **data):
        return SimpleNamespace(topic=topic, timestamp_ns=stamp, data=SimpleNamespace(**data))

    samples = [
        record("/unicycle/unicycle_node/state", T0, state=6, OBSTACLE_STOP=4),
        record("/gps_status", T0 + 1, fix_status="Fixed"),
        record("/unicycle/unicycle_node/state", T0 + 2, state=4, OBSTACLE_STOP=4),
        record("/gps_status", T0 + 3, fix_status="Float"),
        record("/unicycle/unicycle_node/state", T0 + 4, state=24, OBSTACLE_STOP=4),
        record("/unicycle/unicycle_node/state", T0 + 5, state=28, OBSTACLE_STOP=4),
        record("/gps_status", T0 + 6, fix_status="Float"),
    ]
    for key in ["first.mcap", "second.mcap"]:
        (tmp_path / key).touch()
    monkeypatch.setattr(
        job.logs, "open", lambda *args, **kwargs: SimpleNamespace(iter_messages=lambda: iter(samples))
    )
    emitter = CapturingEmitter()
    job.run(tmp_path, emitter, profile="burro")
    for key in ["first.mcap", "second.mcap"]:
        events = [
            (stamp, data)
            for topic, stamp, data in emitter.rows
            if topic == "session_events" and data["recording_key"] == key
        ]
        assert [(stamp, data["name"]) for stamp, data in events] == [
            (T0, "Obstacle stop observed"),
            (T0 + 3, "GPS fix changed"),
            (T0 + 5, "Obstacle stop observed"),
            (T0, "Recording started"),
        ]
        for stamp, data in events:
            assert event_metadata(data["tags"]) == {"recording_key": key, "timestamp_ns": str(stamp)}
    assert {topic for topic, _, _ in emitter.rows} == {
        "session_inventory",
        "session_events",
        "recording_topics",
        "burro_state",
        "burro_gps",
    }


def write_burro_state_mcap(path, constants, *, enabled_state=False, states=(0, 6, 4, 0, 4)):
    typename = "burro_msgs/msg/UnicycleState"
    definition = constants + "\ntime stamp\nuint16 state\n"
    if enabled_state:
        definition += "uint8 enabled_state\n"
    store = get_typestore(Stores.ROS1_NOETIC)
    store.register(get_types_from_msg(definition, typename))
    with path.open("wb") as handle:
        writer = Writer(handle)
        writer.start(profile="ros1")
        schema = writer.register_schema("burro_msgs/UnicycleState", "ros1msg", definition.encode())
        channel = writer.register_channel("/unicycle/unicycle_node/state", "ros1", schema)
        for offset, state in enumerate(states):
            stamp = T0 + offset
            fields = {
                "stamp": store.types["builtin_interfaces/msg/Time"](sec=T0 // NS, nanosec=offset),
                "state": state,
            }
            if enabled_state:
                fields["enabled_state"] = 1
            message = store.types[typename](**fields)
            writer.add_message(channel, stamp, bytes(store.serialize_ros1(message, typename)), stamp)
        schema = writer.register_schema("other", "jsonschema", b'{"type":"object"}')
        channel = writer.register_channel("/other", "json", schema)
        writer.add_message(channel, T0 + 5, b'{"value":1}', T0 + 5)
        writer.finish()


def test_burro_decodes_both_recorded_constant_names_per_file(job, tmp_path):
    write_burro_state_mcap(tmp_path / "legacy.mcap", "uint16 OBSTACLE_STOP=4")
    write_burro_state_mcap(
        tmp_path / "prefixed.mcap", "uint16 STATE_OBSTACLE_STOP=4", enabled_state=True, states=(4, 6, 4, 0, 4)
    )
    emitter = CapturingEmitter()
    job.run(tmp_path, emitter, profile="burro")
    for key in ["legacy.mcap", "prefixed.mcap"]:
        events = [
            (stamp, data)
            for topic, stamp, data in emitter.rows
            if topic == "session_events"
            and data["recording_key"] == key
            and data["name"] == "Obstacle stop observed"
        ]
        first_observation = T0 + 1 if key == "legacy.mcap" else T0
        assert [stamp for stamp, _ in events] == [first_observation, T0 + 4]
        for stamp, data in events:
            assert event_metadata(data["tags"]) == {"recording_key": key, "timestamp_ns": str(stamp)}
        counts = {
            data["topic_name"]: data["message_count"]
            for topic, _, data in emitter.rows
            if topic == "recording_topics" and data["recording_key"] == key
        }
        assert counts == {"/unicycle/unicycle_node/state": 5, "/other": 1}
    inventory = next(data for topic, _, data in emitter.rows if topic == "session_inventory")
    assert inventory["files"] == ["legacy.mcap", "prefixed.mcap"]
    assert inventory["start_ns"] == str(T0)
    assert inventory["end_ns"] == str(T0 + 5)


@pytest.mark.parametrize(
    "constants",
    [
        "",
        "uint16 STATE_OBSTACLE_STOP=0",
        "uint16 STATE_OBSTACLE_STOP=3",
        "bool STATE_OBSTACLE_STOP=true",
        "uint16 OBSTACLE_STOP=4\nuint16 STATE_OBSTACLE_STOP=8",
    ],
)
def test_burro_invalid_decoded_definition_removes_partial_output(job, tmp_path, constants):
    folder = tmp_path / "inputs"
    folder.mkdir()
    write_burro_state_mcap(folder / "a-valid.mcap", "uint16 STATE_OBSTACLE_STOP=4", enabled_state=True)
    write_burro_state_mcap(folder / "z-invalid.mcap", constants, enabled_state=True)
    output = tmp_path / "outputs" / "emissions.resim.jsonl"
    captured = []

    class PartialEmitter(CapturingEmitter):
        def __init__(self, *, config_path, output_path):
            super().__init__()
            captured.append(self)
            Path(output_path).write_text("partial output")

        def close(self):
            pass

    with pytest.raises(ValueError, match="bit field"):
        job.write_output(folder, output, REPO / "config.resim.yml", "burro", PartialEmitter)
    assert any(data.get("recording_key") == "a-valid.mcap" for _, _, data in captured[0].rows)
    assert not output.exists()
    assert not output.with_name(output.name + ".pending").exists()


@pytest.mark.sdk
@pytest.mark.parametrize("name", ["OBSTACLE_STOP", "STATE_OBSTACLE_STOP"])
def test_burro_ros1_entrypoint_with_real_emitter(tmp_path, name):
    folder = tmp_path / "inputs"
    folder.mkdir()
    write_burro_state_mcap(folder / "drive.mcap", f"uint16 {name}=4", enabled_state=True)
    output = tmp_path / "outputs" / "emissions.resim.jsonl"
    subprocess.run(
        [
            sys.executable,
            str(REPO / "job.py"),
            "--robot-profile",
            "burro",
            "--input",
            str(folder),
            "--output",
            str(output),
        ],
        check=True,
    )
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert len([row for row in rows if row["$metadata"].get("event")]) == 3
    assert all(type(row["$metadata"]["timestamp"]) is int for row in rows)


def test_burro_matching_recorded_aliases_are_unambiguous(job):
    assert job.obstacle_stop_mask({"OBSTACLE_STOP": 4, "STATE_OBSTACLE_STOP": 4}) == 4
    with pytest.raises(ValueError, match="bit field"):
        job.obstacle_stop_mask({"OBSTACLE_STOP": 4, "STATE_OBSTACLE_STOP": None})


@pytest.mark.parametrize("mask", [0, 3, -4, True])
def test_burro_requires_a_recorded_single_bit_definition(job, mask):
    record = SimpleNamespace(
        topic="/unicycle/unicycle_node/state", timestamp_ns=T0, data={"state": 4, "OBSTACLE_STOP": mask}
    )
    with pytest.raises(ValueError, match="bit field"):
        job.burro_observation(record, "drive.mcap", CapturingEmitter(), {})


@pytest.mark.parametrize("state", [-1, True, 4.0, "4"])
def test_burro_rejects_invalid_states(job, state):
    record = SimpleNamespace(
        topic="/unicycle/unicycle_node/state", timestamp_ns=T0, data={"state": state, "OBSTACLE_STOP": 4}
    )
    with pytest.raises(ValueError, match="bit field"):
        job.burro_observation(record, "drive.mcap", CapturingEmitter(), {})


@pytest.mark.parametrize("fix", ["", 3, None])
def test_burro_rejects_invalid_gps(job, fix):
    record = SimpleNamespace(topic="/gps_status", timestamp_ns=T0, data={"fix_status": fix})
    with pytest.raises(ValueError, match="GPS"):
        job.burro_observation(record, "drive.mcap", CapturingEmitter(), {})


def test_burro_sql_distinguishes_missing_observations_from_zero():
    config = yaml.safe_load((REPO / "config.resim.yml").read_text())
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE burro_state (stop_observation BOOLEAN)")
        db.execute("CREATE TABLE burro_gps (has_previous BOOLEAN, fix_changed BOOLEAN)")
        stop_query = config["metrics"]["Obstacle stop observations"]["query_string"]
        gps_query = config["metrics"]["GPS fix changes"]["query_string"]
        assert db.execute(stop_query).fetchall() == []
        assert db.execute(gps_query).fetchall() == []
        db.execute("INSERT INTO burro_state VALUES (0)")
        db.execute("INSERT INTO burro_gps VALUES (0, 0), (0, 0)")
        assert db.execute(stop_query).fetchall() == [(0,)]
        assert db.execute(gps_query).fetchall() == []
        db.execute("INSERT INTO burro_gps VALUES (1, 0)")
        assert db.execute(gps_query).fetchall() == [(0,)]
        db.execute("INSERT INTO burro_state VALUES (1), (1)")
        db.execute("INSERT INTO burro_gps VALUES (1, 1)")
        assert db.execute(stop_query).fetchall() == [(2,)]
        assert db.execute(gps_query).fetchall() == [(1,)]
