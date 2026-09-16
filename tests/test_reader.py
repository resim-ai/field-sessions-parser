from __future__ import annotations

import struct

import pytest
from conftest import NS, ROS1, T0, build_ros1_mcap
from mcap.writer import CompressionType, Writer
from protobuf_fixture import (
    STATUS_DESCRIPTORS,
    descriptor_set,
    geometry_file,
    message_class,
    stamped_file,
    status_file,
)
from ros2_fixture import build_ros2_mcap

from field_sessions_parser.reader import UNDECODABLE, LogReader, NoSummaryError, NotMcapError, protobuf_tree
from field_sessions_parser.remote import LocalRangeFile


def open_reader(path, **options) -> tuple[LogReader, LocalRangeFile]:
    stream = LocalRangeFile(path, block_size=options.pop("block_size", 8192))
    reader = LogReader(stream, **options).open()
    return reader, stream


def test_open_reads_header_and_summary(ros1_mcap):
    reader, stream = open_reader(ros1_mcap)
    assert reader.header.profile == "ros1"
    assert reader.header.library == "field-sessions-fixture"
    assert sorted(channel.topic for channel in reader.channels.values()) == [
        "/telemetry",
        "/tf",
        "/vehicle/odom",
        "/vehicle/status",
    ]
    assert reader.indexed
    assert reader.statistics is not None and reader.statistics.message_count == 60 + 60 + 12 + 6
    assert len(reader.chunk_indexes) > 2
    starts = [(ci.message_start_time, ci.chunk_start_offset) for ci in reader.chunk_indexes]
    assert starts == sorted(starts)
    assert stream.requests >= 2


def test_windows_are_the_files_first_and_last_chunk(ros1_mcap):
    reader, stream = open_reader(ros1_mcap, block_size=1024)
    windows = reader.window_chunks()
    assert windows == [reader.chunk_indexes[0], reader.chunk_indexes[-1]]
    assert len(reader.chunk_indexes) > 2
    before = stream.requests
    for chunk_index in windows:
        reader.read_chunk(chunk_index)
    assert reader.chunks_read == 2
    blocks_per_chunk = max((ci.chunk_length // 1024) + 2 for ci in windows)
    assert stream.requests - before <= 2 * blocks_per_chunk


def test_windows_follow_the_wanted_topics(ros1_mcap):
    reader, _ = open_reader(ros1_mcap)
    telemetry_only = reader.window_chunks(topics=["/telemetry"])
    assert 1 <= len(telemetry_only) <= 2
    assert all(
        any(reader.channels[cid].topic == "/telemetry" for cid in ci.message_index_offsets)
        for ci in telemetry_only
    )
    assert reader.window_chunks(topics=["/nope"]) == []


def test_oversized_chunk_is_a_decode_failure_and_never_decompressed(ros1_mcap):
    reader, _ = open_reader(ros1_mcap, max_chunk_uncompressed=100)
    assert reader.chunk_indexes == []
    assert reader.decode_failures
    assert any("uncompressed bytes" in reason for reason in reader.schema_failures.values())
    assert list(reader.iter_messages()) == []
    assert reader.chunks_read == 0


def test_no_summary_names_mcap_recover(no_summary_mcap):
    stream = LocalRangeFile(no_summary_mcap)
    with pytest.raises(NoSummaryError, match="mcap recover"):
        LogReader(stream).open()


def test_not_an_mcap(tmp_path):
    path = tmp_path / "junk.bin"
    path.write_bytes(bytes(range(256)) * 8)
    with pytest.raises(NotMcapError, match="magic"):
        LogReader(LocalRangeFile(path)).open()
    tiny = tmp_path / "tiny.bin"
    tiny.write_bytes(b"\x89MCAP0\r\n")
    with pytest.raises(NotMcapError, match="too small"):
        LogReader(LocalRangeFile(tiny)).open()


def test_unindexed_file_has_no_windows_but_still_iterates(unindexed_mcap):
    reader, _ = open_reader(unindexed_mcap)
    assert not reader.indexed
    assert reader.window_chunks() == []
    messages = list(reader.iter_messages(topics=["/vehicle/odom"]))
    assert len(messages) == 60


def test_iter_messages_is_in_log_time_order_and_complete(ros1_mcap):
    reader, _ = open_reader(ros1_mcap)
    times = [message.log_time for _, message in reader.iter_messages()]
    assert times == sorted(times)
    assert len(times) == reader.statistics.message_count
    odom = [message for channel, message in reader.iter_messages(topics={"/vehicle/odom"}) if channel.topic]
    assert len(odom) == 60
    assert odom[0].log_time == T0
    assert odom[-1].log_time == T0 + int(5.9 * NS)
    assert list(reader.iter_messages(topics=["/nope"])) == []


@pytest.mark.parametrize("compression", [CompressionType.NONE, CompressionType.LZ4, CompressionType.ZSTD])
def test_compressions_decode(tmp_path, compression):
    path = build_ros1_mcap(tmp_path / f"{compression.name}.mcap", seconds=1.0, compression=compression)
    reader, _ = open_reader(path)
    channel, message = next(reader.iter_messages(topics=["/vehicle/odom"]))
    decoded = reader.decode(channel, message)
    assert decoded.twist.twist.linear.x == 1.0


def test_ros1_custom_and_json_channels_decode(ros1_mcap):
    reader, _ = open_reader(ros1_mcap)
    by_topic = {channel.topic: channel for channel in reader.channels.values()}
    for channel, message in reader.iter_messages(topics=["/vehicle/status"]):
        decoded = reader.decode(channel, message)
        assert decoded.label.startswith("leg-")
        assert decoded.nested.gain >= 0
        break
    for channel, message in reader.iter_messages(topics=["/telemetry"]):
        decoded = reader.decode(channel, message)
        assert decoded == {"cpu": 0.5, "note": "ok", "nested": {"depth": 0}}
        break
    assert reader.decoder_for(by_topic["/tf"]) is not None
    assert reader.decode_failures == {}
    assert reader.schema_failures == {}


def test_protobuf_channel_whose_descriptors_lack_its_type_is_unsupported(ros1_mixed_mcap):
    reader, _ = open_reader(ros1_mixed_mcap)
    proto = next(channel for channel in reader.channels.values() if channel.topic == "/proto")
    assert reader.decoder_for(proto) is None
    assert reader.decoder_name(proto) is None
    assert not reader.encoding_supported(proto)
    assert reader.schema_failures["/proto"] == (
        "unsupported encoding 'protobuf': the FileDescriptorSet does not define 'demo.Proto'"
    )
    channel, message = next(reader.iter_messages(topics=["/proto"]))
    assert reader.decode(channel, message) is UNDECODABLE
    assert reader.decode_failures == {"/proto": 1}


def test_protobuf_channel_with_descriptors_decodes_to_a_tree(protobuf_status_mcap):
    reader, _ = open_reader(protobuf_status_mcap)
    by_topic = {channel.topic: channel for channel in reader.channels.values()}
    assert reader.decoder_name(by_topic["/robot/status"]) == "builtin:protobuf"
    assert reader.decoder_name(by_topic["/robot/raw"]) is None
    assert not reader.encoding_supported(by_topic["/robot/raw"])
    assert reader.schema_failures == {
        "/robot/raw": "unsupported encoding 'protobuf': no protobuf FileDescriptorSet schema (found 'none')"
    }
    messages = list(reader.iter_messages(topics=["/robot/status"]))
    assert len(messages) == 60
    first = reader.decode(*messages[0])
    assert list(first) == [
        "timestamp_ns",
        "speed_mps",
        "mode",
        "armed",
        "label",
        "velocity",
        "waypoints",
        "payload",
        "counts",
        "battery_v",
        "params",
        "parent",
    ]
    assert first["timestamp_ns"] == T0 and isinstance(first["timestamp_ns"], int)
    assert first["mode"] == "MODE_IDLE" and first["armed"] is True and first["label"] == "leg-0"
    assert first["velocity"] == {"x": 1.0, "y": 0.0, "z": 0.1}
    assert first["waypoints"] == [{"x": 0.0, "y": 2.0, "z": 0.0}]
    assert first["payload"] == b"\x00\xff\x00" and first["counts"] == [0, 1]
    assert first["params"] == {"gain": 0.0} and first["battery_v"] == 48.0
    assert first["parent"] is None
    second = reader.decode(*messages[1])
    assert second["mode"] == "MODE_DRIVE"
    assert reader.decode(*messages[25])["speed_mps"] == 0.0
    assert reader.decode_failures == {}


def test_protobuf_tree_ends_a_recursive_type_at_the_first_unset_level():
    status = message_class(STATUS_DESCRIPTORS, "demo.Status")(label="child")
    status.parent.label = "root"
    tree = protobuf_tree(status)
    assert tree["label"] == "child" and tree["parent"]["label"] == "root"
    assert tree["parent"]["parent"] is None and tree["parent"]["velocity"] is None
    assert tree["parent"]["payload"] == b"" and tree["parent"]["params"] == {}


def test_protobuf_well_known_imports_come_from_the_runtime_when_the_set_omits_them(tmp_path):
    descriptors = descriptor_set(stamped_file())
    assert b"google/protobuf/timestamp.proto" in descriptors and b"seconds" not in descriptors
    stamped = message_class(descriptors, "demo.Stamped")(name="x")
    stamped.stamp.seconds, stamped.stamp.nanos = 5, 7
    stamped.api.name = "demo.Api"
    path = tmp_path / "proto-well-known.mcap"
    with open(path, "wb") as handle:
        writer = Writer(handle, chunk_size=1024)
        writer.start(profile="", library="field-sessions-fixture")
        channel = writer.register_channel(
            "/stamped", "protobuf", writer.register_schema("demo.Stamped", "protobuf", descriptors)
        )
        writer.add_message(channel, T0, stamped.SerializeToString(), T0, 0)
        writer.finish()
    reader, _ = open_reader(path)
    channel = next(iter(reader.channels.values()))
    assert reader.decoder_name(channel) == "builtin:protobuf"
    tree = reader.decode(*next(reader.iter_messages()))
    assert tree["stamp"] == {"seconds": 5, "nanos": 7} and tree["name"] == "x"
    assert tree["api"]["name"] == "demo.Api" and tree["api"]["source_context"] is None
    assert reader.schema_failures == {} and reader.decode_failures == {}


def test_protobuf_descriptor_files_load_in_dependency_order_and_garbage_is_named(tmp_path):
    reversed_set = descriptor_set(status_file(), geometry_file())
    status = message_class(STATUS_DESCRIPTORS, "demo.Status")(label="x")
    path = tmp_path / "proto-order.mcap"
    with open(path, "wb") as handle:
        writer = Writer(handle, chunk_size=1024)
        writer.start(profile="", library="field-sessions-fixture")
        ordered = writer.register_channel(
            "/ordered", "protobuf", writer.register_schema("demo.Status", "protobuf", reversed_set)
        )
        garbage = writer.register_channel(
            "/garbage", "protobuf", writer.register_schema("demo.Status", "protobuf", b"\xff\xff\xff")
        )
        orphan = writer.register_channel(
            "/orphan",
            "protobuf",
            writer.register_schema("demo.Status", "protobuf", descriptor_set(status_file())),
        )
        for channel in (ordered, garbage, orphan):
            writer.add_message(channel, T0, status.SerializeToString(), T0, 0)
        writer.finish()
    reader, _ = open_reader(path)
    by_topic = {channel.topic: channel for channel in reader.channels.values()}
    assert reader.decoder_name(by_topic["/ordered"]) == "builtin:protobuf"
    channel, message = next(reader.iter_messages(topics=["/ordered"]))
    assert reader.decode(channel, message)["label"] == "x"
    assert reader.decoder_name(by_topic["/garbage"]) is None
    assert reader.schema_failures["/garbage"].startswith(
        "unsupported encoding 'protobuf': the schema is not a FileDescriptorSet ("
    )
    assert reader.decoder_name(by_topic["/orphan"]) is None
    assert reader.schema_failures["/orphan"].startswith(
        "unsupported encoding 'protobuf': the FileDescriptorSet cannot be loaded ("
    )
    assert "demo/geometry.proto" in reader.schema_failures["/orphan"]


def test_ros2idl_schema_registers_and_decodes(ros2idl_mcap):
    reader, _ = open_reader(ros2idl_mcap)
    channel = next(iter(reader.channels.values()))
    assert channel.message_encoding == "cdr" and reader.schema_for(channel).encoding == "ros2idl"
    assert reader.decoder_name(channel) == "builtin:ros2idl"
    messages = list(reader.iter_messages(topics=["/robot/status"]))
    assert len(messages) == 12
    decoded = reader.decode(*messages[3])
    assert decoded.label == "leg-3" and decoded.mode == 0 and decoded.armed is False
    assert decoded.battery_v == 48.0 - 0.3
    assert decoded.nested.gain == 0.75 and decoded.nested.blob.tolist() == [3, 7]
    assert decoded.covariance.tolist() == [0.0, 0.0, 0.0, 0.0]
    assert reader.decode_failures == {} and reader.schema_failures == {}


def test_decoder_names_follow_the_encoding_pair(ros1_mcap, tmp_path):
    reader, _ = open_reader(ros1_mcap)
    names = {channel.topic: reader.decoder_name(channel) for channel in reader.channels.values()}
    assert names == {
        "/vehicle/odom": "builtin:ros1",
        "/tf": "builtin:ros1",
        "/vehicle/status": "builtin:ros1",
        "/telemetry": "builtin:json",
    }
    reader, _ = open_reader(build_ros2_mcap(tmp_path / "ros2.mcap"))
    by_topic = {channel.topic: channel for channel in reader.channels.values()}
    assert reader.decoder_name(by_topic["/odom"]) == "builtin:cdr"
    assert reader.decoder_name(by_topic["/broken"]) is None
    assert reader.encoding_supported(by_topic["/broken"])


def test_ros2_fixture_decodes_and_rejects_the_broken_schema(tmp_path):
    path = build_ros2_mcap(tmp_path / "ros2.mcap")
    reader, _ = open_reader(path)
    assert reader.header.profile == "ros2"
    odom_messages = list(reader.iter_messages(topics=["/odom"]))
    assert len(odom_messages) == 60
    channel, message = odom_messages[25]
    decoded = reader.decode(channel, message)
    assert decoded.twist.twist.linear.x == 0.0
    assert decoded.header.frame_id == "odom"

    broken = list(reader.iter_messages(topics=["/broken"]))
    assert len(broken) == 1
    channel, message = broken[0]
    assert reader.decode(channel, message) is UNDECODABLE
    assert reader.decode_failures["/broken"] == 1
    assert "mystery_pkg" in reader.schema_failures["demo_msgs/msg/Broken"]


FOOTER_RECORD = 1 + 8 + 8 + 8 + 4


def footer_summary_start(data: bytes) -> int:
    footer_start = len(data) - 8 - FOOTER_RECORD
    return int.from_bytes(data[footer_start + 9 : footer_start + 17], "little")


def with_summary_start(data: bytes, summary_start: int) -> bytes:
    patched = bytearray(data)
    footer_start = len(data) - 8 - FOOTER_RECORD
    patched[footer_start + 9 : footer_start + 17] = summary_start.to_bytes(8, "little")
    return bytes(patched)


def test_linear_file_without_chunk_indexes_iterates_the_data_section(linear_mcap):
    reader, _ = open_reader(linear_mcap)
    assert reader.chunk_indexes == [] and not reader.indexed
    assert reader.window_chunks() == []
    times = [message.log_time for _, message in reader.iter_messages()]
    assert len(times) == 138 and times == sorted(times)
    odom = list(reader.iter_messages(topics=["/vehicle/odom"]))
    assert len(odom) == 60 and odom[0][1].log_time == T0
    assert reader.chunks_read == 0


def test_chunk_record_disagreeing_with_its_index_is_rejected_before_its_body_is_read(ros1_mcap, tmp_path):
    reader, _ = open_reader(ros1_mcap)
    first = reader.chunk_indexes[0]
    held = {reader.channels[cid].topic: 1 for cid in first.message_index_offsets}
    header = first.chunk_start_offset + 9
    sizes = {
        "uncompressed": (header + 16, first.uncompressed_size + 1),
        "data_length": (header + 32 + len(first.compression), 2**40),
    }
    for label, (offset, value) in sizes.items():
        data = bytearray(ros1_mcap.read_bytes())
        data[offset : offset + 8] = value.to_bytes(8, "little")
        patched = tmp_path / f"{label}.mcap"
        patched.write_bytes(data)
        reader, stream = open_reader(patched, block_size=1024)
        before = stream.requests
        assert reader.read_chunk(reader.chunk_indexes[0]) == []
        assert stream.requests == before
        assert reader.decode_failures == held
        reasons = list(reader.schema_failures.values())
        assert any("declares" in reason and "its index" in reason for reason in reasons)


def test_summary_start_inside_the_data_section_never_decompresses_a_chunk(ros1_mcap, tmp_path, monkeypatch):
    patched = tmp_path / "early-summary.mcap"
    patched.write_bytes(with_summary_start(ros1_mcap.read_bytes(), 8))
    monkeypatch.setattr(
        "mcap.stream_reader.breakup_chunk",
        lambda *args, **kwargs: pytest.fail("a chunk was decompressed while reading the summary"),
    )
    reader, _ = open_reader(patched)
    assert len(reader.channels) == 4 and len(reader.chunk_indexes) > 2


def test_summary_over_the_cap_is_rejected(ros1_mcap, tmp_path):
    data = ros1_mcap.read_bytes()
    cap = len(data) - footer_summary_start(data) + 256
    reader, _ = open_reader(ros1_mcap, max_summary_bytes=cap)
    assert len(reader.channels) == 4
    patched = tmp_path / "early-summary.mcap"
    patched.write_bytes(with_summary_start(data, 8))
    with pytest.raises(NoSummaryError, match="over the"):
        open_reader(patched, max_summary_bytes=cap)


def build_conflicting_schema_mcap(path):
    with open(path, "wb") as handle:
        writer = Writer(handle, chunk_size=1024)
        writer.start(profile="ros1", library="field-sessions-fixture")
        first = writer.register_schema("demo_msgs/Pair", "ros1msg", b"float64 a\nint32 b\n")
        second = writer.register_schema("demo_msgs/Pair", "ros1msg", b"int32 b\nfloat64 a\n")
        pair_a = writer.register_channel("/pair_a", "ros1", first)
        pair_b = writer.register_channel("/pair_b", "ros1", second)
        for step in range(5):
            t_ns = T0 + step * NS
            writer.add_message(pair_a, t_ns, struct.pack("<di", 1.5, 7), t_ns, step)
            writer.add_message(pair_b, t_ns, struct.pack("<id", 7, 1.5), t_ns, step)
        writer.finish()
    return path


def test_conflicting_type_definition_is_a_schema_failure_not_a_wrong_decode(tmp_path):
    reader, _ = open_reader(build_conflicting_schema_mcap(tmp_path / "conflict.mcap"))
    by_topic = {channel.topic: channel for channel in reader.channels.values()}
    _, first = next(iter(reader.iter_messages(topics=["/pair_a"])))
    decoded = reader.decode(by_topic["/pair_a"], first)
    assert (decoded.a, decoded.b) == (1.5, 7)
    _, second = next(iter(reader.iter_messages(topics=["/pair_b"])))
    assert reader.decode(by_topic["/pair_b"], second) is UNDECODABLE
    assert reader.decode_failures == {"/pair_b": 1}
    assert "already present with different definition" in reader.schema_failures["demo_msgs/Pair"]


@pytest.mark.parametrize("dependency_first", [False, True])
@pytest.mark.parametrize(
    ("first_constants", "second_constants", "compatible"),
    [
        ("", "uint8 STOP=4\n", False),
        ("uint8 STOP=4\n", "", False),
        ("uint8 STOP=4\n", "uint8 STOP=8\n", False),
        ("uint8 STOP=4\n", "uint8 STATE_STOP=4\n", False),
        ("uint8 STOP=4\n", "uint16 STOP=4\n", False),
        ("uint8 STOP=4\n", "uint8 STOP=4\nuint8 STOP=4\n", False),
        ("uint8 STOP=4\nuint8 SLOW=8\n", "uint8 SLOW=8\nuint8 STOP=4\n", True),
    ],
)
def test_ros_constants_cannot_change_with_schema_registration_order(
    tmp_path, dependency_first, first_constants, second_constants, compatible
):
    path = tmp_path / "constants.mcap"
    first_name = "demo_msgs/Envelope" if dependency_first else "demo_msgs/State"
    first_definition = first_constants + "uint8 state\n"
    if dependency_first:
        first_definition = (
            "demo_msgs/State state\n" + "=" * 80 + "\nMSG: demo_msgs/State\n" + first_definition
        )
    with path.open("wb") as handle:
        writer = Writer(handle)
        writer.start(profile="ros1")
        first_schema = writer.register_schema(first_name, "ros1msg", first_definition.encode())
        second_schema = writer.register_schema(
            "demo_msgs/State", "ros1msg", (second_constants + "uint8 state\n").encode()
        )
        for offset, schema in enumerate([first_schema, second_schema]):
            channel = writer.register_channel(f"/state{offset}", "ros1", schema)
            writer.add_message(channel, T0 + offset, b"\x04", T0 + offset)
        writer.finish()
    reader, _ = open_reader(path)
    messages = list(reader.iter_messages())
    first = reader.decode(*messages[0])
    assert first is not UNDECODABLE
    first_state = first.state if dependency_first else first
    original_mask = getattr(first_state, "STOP", None)
    second = reader.decode(*messages[1])
    if compatible:
        assert second.state == 4 and second.STOP == 4 and second.SLOW == 8
        assert not reader.schema_failures
    else:
        assert second is UNDECODABLE
        assert "constants" in reader.schema_failures["demo_msgs/State"]
    assert getattr(first_state, "STOP", None) == original_mask


def test_ros_builtin_constants_cannot_be_silently_overridden(tmp_path):
    definition = ROS1.generate_msgdef("sensor_msgs/msg/NavSatStatus")[0]
    changed = definition.replace("STATUS_FIX=0", "STATUS_FIX=7")
    assert changed != definition
    path = tmp_path / "builtin.mcap"
    with path.open("wb") as handle:
        writer = Writer(handle)
        writer.start(profile="ros1")
        schema = writer.register_schema("sensor_msgs/NavSatStatus", "ros1msg", changed.encode())
        channel = writer.register_channel("/fix", "ros1", schema)
        writer.add_message(channel, T0, struct.pack("<bH", 0, 1), T0)
        writer.finish()
    reader, _ = open_reader(path)
    assert reader.decode(*next(reader.iter_messages())) is UNDECODABLE
    assert "different constants" in reader.schema_failures["sensor_msgs/NavSatStatus"]
