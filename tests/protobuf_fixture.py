"""A protobuf MCAP whose schema is a FileDescriptorSet built by hand, no protoc: 10 Hz status, one stop."""

from __future__ import annotations

from pathlib import Path

from conftest import NS, T0, speed_at
from google.protobuf import (
    any_pb2,
    api_pb2,
    descriptor_pb2,
    descriptor_pool,
    message_factory,
    source_context_pb2,
    timestamp_pb2,
    type_pb2,
)
from mcap.writer import Writer

FIELD = descriptor_pb2.FieldDescriptorProto
MODES = ("MODE_IDLE", "MODE_DRIVE", "MODE_FAULT")
STATUS_HZ = 10
WELL_KNOWN_MODULES = (timestamp_pb2, any_pb2, source_context_pb2, type_pb2, api_pb2)


def _file(name: str, package: str, dependencies: tuple[str, ...] = ()) -> descriptor_pb2.FileDescriptorProto:
    file = descriptor_pb2.FileDescriptorProto(name=name, package=package, syntax="proto3")
    file.dependency.extend(dependencies)
    return file


def _scalar(message, name: str, number: int, type_: int, *, repeated: bool = False) -> None:
    message.field.add(
        name=name, number=number, type=type_, label=FIELD.LABEL_REPEATED if repeated else FIELD.LABEL_OPTIONAL
    )


def _message_field(message, name: str, number: int, type_name: str, *, repeated: bool = False) -> None:
    message.field.add(
        name=name,
        number=number,
        type=FIELD.TYPE_MESSAGE,
        type_name=type_name,
        label=FIELD.LABEL_REPEATED if repeated else FIELD.LABEL_OPTIONAL,
    )


def geometry_file() -> descriptor_pb2.FileDescriptorProto:
    file = _file("demo/geometry.proto", "demo.geometry")
    vector = file.message_type.add(name="Vector3")
    for number, axis in enumerate(("x", "y", "z"), start=1):
        _scalar(vector, axis, number, FIELD.TYPE_DOUBLE)
    return file


def status_file() -> descriptor_pb2.FileDescriptorProto:
    file = _file("demo/status.proto", "demo", ("demo/geometry.proto",))
    mode = file.enum_type.add(name="Mode")
    for number, name in enumerate(MODES):
        mode.value.add(name=name, number=number)
    status = file.message_type.add(name="Status")
    _scalar(status, "timestamp_ns", 1, FIELD.TYPE_UINT64)
    _scalar(status, "speed_mps", 2, FIELD.TYPE_DOUBLE)
    status.field.add(
        name="mode", number=3, type=FIELD.TYPE_ENUM, type_name=".demo.Mode", label=FIELD.LABEL_OPTIONAL
    )
    _scalar(status, "armed", 4, FIELD.TYPE_BOOL)
    _scalar(status, "label", 5, FIELD.TYPE_STRING)
    _message_field(status, "velocity", 6, ".demo.geometry.Vector3")
    _message_field(status, "waypoints", 7, ".demo.geometry.Vector3", repeated=True)
    _scalar(status, "payload", 8, FIELD.TYPE_BYTES)
    _scalar(status, "counts", 9, FIELD.TYPE_INT32, repeated=True)
    _scalar(status, "battery_v", 10, FIELD.TYPE_FLOAT)
    entry = status.nested_type.add(name="ParamsEntry")
    entry.options.map_entry = True
    _scalar(entry, "key", 1, FIELD.TYPE_STRING)
    _scalar(entry, "value", 2, FIELD.TYPE_DOUBLE)
    _message_field(status, "params", 11, ".demo.Status.ParamsEntry", repeated=True)
    _message_field(status, "parent", 12, ".demo.Status")
    return file


def stamped_file() -> descriptor_pb2.FileDescriptorProto:
    """Imports two well-known files a set written without `--include_imports` leaves out."""
    file = _file(
        "demo/stamped.proto", "demo", ("google/protobuf/timestamp.proto", "google/protobuf/api.proto")
    )
    stamped = file.message_type.add(name="Stamped")
    _message_field(stamped, "stamp", 1, ".google.protobuf.Timestamp")
    _scalar(stamped, "name", 2, FIELD.TYPE_STRING)
    _message_field(stamped, "api", 3, ".google.protobuf.Api")
    return file


def descriptor_set(*files: descriptor_pb2.FileDescriptorProto) -> bytes:
    """The files in the order given, so a set can list a file before its dependency."""
    return descriptor_pb2.FileDescriptorSet(file=files).SerializeToString()


STATUS_DESCRIPTORS = descriptor_set(geometry_file(), status_file())


def message_class(descriptors: bytes, full_name: str) -> type:
    """The class from a pool holding the runtime's well-known files first, as protoc's own does."""
    pool = descriptor_pool.DescriptorPool()
    for module in WELL_KNOWN_MODULES:
        pool.AddSerializedFile(module.DESCRIPTOR.serialized_pb)
    for file in descriptor_pb2.FileDescriptorSet.FromString(descriptors).file:
        pool.AddSerializedFile(file.SerializeToString())
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(full_name))


def status_message(status_class: type, t_s: float, step: int) -> bytes:
    message = status_class(
        timestamp_ns=T0 + int(round(t_s * NS)),
        speed_mps=speed_at(t_s),
        mode=step % len(MODES),
        armed=step % 2 == 0,
        label=f"leg-{step}",
        payload=bytes((step % 256, 0xFF, 0x00)),
        battery_v=48.0 - 0.5 * step,
    )
    message.velocity.x = speed_at(t_s)
    message.velocity.z = 0.1
    message.waypoints.add(x=1.0 * step, y=2.0)
    message.counts.extend([step, step + 1])
    message.params["gain"] = 0.25 * step
    return message.SerializeToString(deterministic=True)


def build_protobuf_mcap(path: Path, *, seconds: float = 6.0, chunk_size: int = 2048) -> Path:
    """`/robot/status` with descriptors and `/robot/raw`, protobuf with no schema at all."""
    status_class = message_class(STATUS_DESCRIPTORS, "demo.Status")
    with open(path, "wb") as handle:
        writer = Writer(handle, chunk_size=chunk_size)
        writer.start(profile="", library="field-sessions-fixture")
        schema = writer.register_schema("demo.Status", "protobuf", STATUS_DESCRIPTORS)
        status_channel = writer.register_channel("/robot/status", "protobuf", schema)
        raw_channel = writer.register_channel("/robot/raw", "protobuf", 0)
        for step in range(int(seconds * STATUS_HZ)):
            t_s = step / STATUS_HZ
            t_ns = T0 + int(round(t_s * NS))
            writer.add_message(status_channel, t_ns, status_message(status_class, t_s, step), t_ns, step)
            if step == 0:
                writer.add_message(raw_channel, t_ns, b"\x08\x01", t_ns, 0)
        writer.finish()
    return path


PROTOBUF_PLAN = {
    "tags": ["terrain"],
    "channels": [
        {
            "topic": "/robot/status",
            "fields": [
                {"path": "speed_mps", "column": "speed", "type": "double"},
                {"path": "timestamp_ns", "column": "stamp_ns", "type": "bigint"},
                {"path": "mode", "type": "string"},
                {"path": "armed", "type": "boolean"},
                {"path": "velocity.x", "column": "vx", "type": "double"},
                {"path": "params.gain", "column": "gain", "type": "double"},
                {"path": "waypoints.0.y", "column": "first_waypoint_y", "type": "double"},
            ],
        }
    ],
    "events": [
        {
            "name": "stopped",
            "channel": "/robot/status",
            "field": "speed_mps",
            "below": 0.05,
            "held_for_s": 1.0,
            "status": "FAIL_WARN",
            "tags": ["stop"],
            "description": "Vehicle stationary",
        }
    ],
}
