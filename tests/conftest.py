"""Synthetic ROS 1 MCAP fixtures: 10 Hz odometry with one stop, tf, a custom message and a JSON channel."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
from mcap.writer import CompressionType, IndexType, Writer
from rosbags.typesys import Stores, get_types_from_msg, get_typestore

collect_ignore_glob = ["fixtures/*"]

NS = 1_000_000_000
T0 = 1_700_000_000 * NS
ODOM_HZ = 10
STATUS_HZ = 2
TELEMETRY_HZ = 1
STOP_START_S = 2.0
STOP_END_S = 4.0
DIP_S = 5.0

CUSTOM_STATUS_MSG = (
    "std_msgs/Header header\n"
    "float64 battery_v\n"
    "int32 mode\n"
    "bool armed\n"
    "string label\n"
    "demo_msgs/Nested nested\n" + "=" * 80 + "\nMSG: demo_msgs/Nested\n"
    "float32 gain\n"
    "uint8[] blob\n"
)

TF_MSG = "geometry_msgs/TransformStamped[] transforms\n"

ROS1 = get_typestore(Stores.ROS1_NOETIC)
ROS1.register(get_types_from_msg(CUSTOM_STATUS_MSG, "demo_msgs/msg/Status"))
ROS1.register(get_types_from_msg(TF_MSG, "tf2_msgs/msg/TFMessage"))


def speed_at(t_s: float) -> float:
    if STOP_START_S <= t_s < STOP_END_S:
        return 0.0
    if abs(t_s - DIP_S) < 1e-9:
        return 0.02
    return 1.0


def odometry(t_s: float, distance: float, seq: int):
    types = ROS1.types
    stamp = types["builtin_interfaces/msg/Time"](sec=int(T0 // NS + int(t_s)), nanosec=int((t_s % 1) * NS))
    header = types["std_msgs/msg/Header"](seq=seq, stamp=stamp, frame_id="odom")
    point = types["geometry_msgs/msg/Point"](x=distance, y=0.0, z=0.0)
    quat = types["geometry_msgs/msg/Quaternion"](x=0.0, y=0.0, z=0.0, w=1.0)
    pose = types["geometry_msgs/msg/PoseWithCovariance"](
        pose=types["geometry_msgs/msg/Pose"](position=point, orientation=quat),
        covariance=np.zeros(36),
    )
    linear = types["geometry_msgs/msg/Vector3"](x=speed_at(t_s), y=0.0, z=0.0)
    angular = types["geometry_msgs/msg/Vector3"](x=0.0, y=0.0, z=0.1)
    twist = types["geometry_msgs/msg/TwistWithCovariance"](
        twist=types["geometry_msgs/msg/Twist"](linear=linear, angular=angular),
        covariance=np.full(36, float("nan")),
    )
    return types["nav_msgs/msg/Odometry"](header=header, child_frame_id="base_link", pose=pose, twist=twist)


def tf_message(t_s: float, distance: float, seq: int):
    types = ROS1.types
    stamp = types["builtin_interfaces/msg/Time"](sec=int(T0 // NS + int(t_s)), nanosec=int((t_s % 1) * NS))
    header = types["std_msgs/msg/Header"](seq=seq, stamp=stamp, frame_id="odom")
    transform = types["geometry_msgs/msg/Transform"](
        translation=types["geometry_msgs/msg/Vector3"](x=distance, y=0.0, z=0.0),
        rotation=types["geometry_msgs/msg/Quaternion"](x=0.0, y=0.0, z=0.0, w=1.0),
    )
    stamped = types["geometry_msgs/msg/TransformStamped"](
        header=header, child_frame_id="base_link", transform=transform
    )
    return types["tf2_msgs/msg/TFMessage"](transforms=[stamped])


def status_message(t_s: float, seq: int):
    types = ROS1.types
    stamp = types["builtin_interfaces/msg/Time"](sec=int(T0 // NS + int(t_s)), nanosec=int((t_s % 1) * NS))
    header = types["std_msgs/msg/Header"](seq=seq, stamp=stamp, frame_id="base_link")
    nested = types["demo_msgs/msg/Nested"](gain=0.25 * seq, blob=np.array([1, 2, 3], dtype=np.uint8))
    return types["demo_msgs/msg/Status"](
        header=header,
        battery_v=48.0 - 0.1 * seq,
        mode=seq % 3,
        armed=seq % 2 == 0,
        label=f"leg-{seq}",
        nested=nested,
    )


def build_ros1_mcap(
    path: Path,
    *,
    seconds: float = 6.0,
    chunk_size: int = 2048,
    compression: CompressionType = CompressionType.ZSTD,
    index_types: IndexType = IndexType.ALL,
    use_statistics: bool = True,
    repeat_channels: bool = True,
    repeat_schemas: bool = True,
    use_summary_offsets: bool = True,
    with_protobuf: bool = False,
) -> Path:
    with open(path, "wb") as handle:
        writer = Writer(
            handle,
            chunk_size=chunk_size,
            compression=compression,
            index_types=index_types,
            use_statistics=use_statistics,
            repeat_channels=repeat_channels,
            repeat_schemas=repeat_schemas,
            use_summary_offsets=use_summary_offsets,
        )
        writer.start(profile="ros1", library="field-sessions-fixture")
        odom_schema = writer.register_schema(
            "nav_msgs/Odometry", "ros1msg", ROS1.generate_msgdef("nav_msgs/msg/Odometry")[0].encode()
        )
        tf_schema = writer.register_schema(
            "tf2_msgs/TFMessage", "ros1msg", ROS1.generate_msgdef("tf2_msgs/msg/TFMessage")[0].encode()
        )
        status_schema = writer.register_schema("demo_msgs/Status", "ros1msg", CUSTOM_STATUS_MSG.encode())
        telemetry_schema = writer.register_schema("telemetry", "jsonschema", b'{"type":"object"}')
        odom_channel = writer.register_channel("/vehicle/odom", "ros1", odom_schema)
        tf_channel = writer.register_channel("/tf", "ros1", tf_schema)
        status_channel = writer.register_channel("/vehicle/status", "ros1", status_schema)
        telemetry_channel = writer.register_channel("/telemetry", "json", telemetry_schema)
        if with_protobuf:
            proto_schema = writer.register_schema("demo.Proto", "protobuf", b"\x0a\x00")
            proto_channel = writer.register_channel("/proto", "protobuf", proto_schema)

        distance = 0.0
        steps = int(seconds * ODOM_HZ)
        for step in range(steps):
            t_s = step / ODOM_HZ
            t_ns = T0 + int(round(t_s * NS))
            distance += speed_at(t_s) / ODOM_HZ
            writer.add_message(
                odom_channel,
                t_ns,
                ROS1.serialize_ros1(odometry(t_s, distance, step), "nav_msgs/msg/Odometry"),
                t_ns,
                step,
            )
            writer.add_message(
                tf_channel,
                t_ns,
                ROS1.serialize_ros1(tf_message(t_s, distance, step), "tf2_msgs/msg/TFMessage"),
                t_ns,
                step,
            )
            if step % (ODOM_HZ // STATUS_HZ) == 0:
                seq = step // (ODOM_HZ // STATUS_HZ)
                writer.add_message(
                    status_channel,
                    t_ns,
                    ROS1.serialize_ros1(status_message(t_s, seq), "demo_msgs/msg/Status"),
                    t_ns,
                    seq,
                )
            if step % (ODOM_HZ // TELEMETRY_HZ) == 0:
                payload = {"cpu": 0.5 + 0.01 * step, "note": "ok", "nested": {"depth": step}}
                writer.add_message(telemetry_channel, t_ns, json.dumps(payload).encode(), t_ns, step)
            if with_protobuf and step == 0:
                writer.add_message(proto_channel, t_ns, b"\x08\x01", t_ns, 0)
        writer.finish()
    return path


def build_protobuf_only_mcap(path: Path) -> Path:
    with open(path, "wb") as handle:
        writer = Writer(handle, chunk_size=1024)
        writer.start(profile="", library="field-sessions-fixture")
        schema = writer.register_schema("demo.Proto", "protobuf", b"\x0a\x00")
        channel = writer.register_channel("/proto", "protobuf", schema)
        for step in range(10):
            t_ns = T0 + step * NS // 10
            writer.add_message(channel, t_ns, b"\x08\x01", t_ns, step)
        writer.finish()
    return path


def build_no_summary_mcap(path: Path) -> Path:
    return build_ros1_mcap(
        path,
        seconds=1.0,
        index_types=IndexType.NONE,
        use_statistics=False,
        repeat_channels=False,
        repeat_schemas=False,
        use_summary_offsets=False,
    )


def build_unindexed_mcap(path: Path) -> Path:
    return build_ros1_mcap(path, index_types=IndexType.CHUNK)


def build_linear_mcap(path: Path) -> Path:
    return build_ros1_mcap(path, index_types=IndexType.NONE)


@pytest.fixture(scope="session")
def ros1_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_ros1_mcap(tmp_path_factory.mktemp("fixtures") / "ros1.mcap")


@pytest.fixture(scope="session")
def long_ros1_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("fixtures") / "ros1-long.mcap"
    return build_ros1_mcap(path, seconds=60.0, chunk_size=16384)


@pytest.fixture(scope="session")
def ros1_mixed_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_ros1_mcap(tmp_path_factory.mktemp("fixtures") / "ros1-mixed.mcap", with_protobuf=True)


@pytest.fixture(scope="session")
def protobuf_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_protobuf_only_mcap(tmp_path_factory.mktemp("fixtures") / "proto.mcap")


@pytest.fixture(scope="session")
def no_summary_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_no_summary_mcap(tmp_path_factory.mktemp("fixtures") / "nosummary.mcap")


@pytest.fixture(scope="session")
def unindexed_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_unindexed_mcap(tmp_path_factory.mktemp("fixtures") / "unindexed.mcap")


@pytest.fixture(scope="session")
def linear_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_linear_mcap(tmp_path_factory.mktemp("fixtures") / "linear.mcap")


@pytest.fixture(scope="session")
def protobuf_status_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from protobuf_fixture import build_protobuf_mcap

    return build_protobuf_mcap(tmp_path_factory.mktemp("fixtures") / "protobuf-status.mcap")


@pytest.fixture(scope="session")
def ros2idl_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from ros2idl_fixture import build_ros2idl_mcap

    return build_ros2idl_mcap(tmp_path_factory.mktemp("fixtures") / "ros2idl.mcap")
