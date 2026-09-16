"""Generate only synthetic ROS 1 state + JSON GPS MCAPs; no network or SDK."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from mcap.writer import Writer
from rosbags.typesys import Stores, get_types_from_msg, get_typestore

START = 1789420800000000001


def write_recording(path: Path, constant: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    typename = "burro_msgs/msg/UnicycleState"
    definition = f"uint16 {constant}=4\ntime stamp\nuint16 state\nuint8 enabled_state\n"
    store = get_typestore(Stores.ROS1_NOETIC)
    store.register(get_types_from_msg(definition, typename))
    with path.open("wb") as handle:
        writer = Writer(handle)
        writer.start(profile="ros1", library="signal-flag-synthetic-sdk-check")
        schema = writer.register_schema("burro_msgs/UnicycleState", "ros1msg", definition.encode())
        state_channel = writer.register_channel("/unicycle/unicycle_node/state", "ros1", schema)
        json_schema = writer.register_schema("synthetic-json", "jsonschema", b'{"type":"object"}')
        gps_channel = writer.register_channel("/gps_status", "json", json_schema)
        other_channel = writer.register_channel("/other", "json", json_schema)
        for offset, state in enumerate([0, 6, 4, 0, 4]):
            stamp = START + offset
            message = store.types[typename](
                stamp=store.types["builtin_interfaces/msg/Time"](
                    sec=stamp // 1_000_000_000, nanosec=stamp % 1_000_000_000
                ),
                state=state,
                enabled_state=1,
            )
            writer.add_message(state_channel, stamp, bytes(store.serialize_ros1(message, typename)), stamp)
        for offset, fix in enumerate(["Fixed", "Fixed", "Float", "Fixed"], start=5):
            stamp = START + offset
            writer.add_message(gps_channel, stamp, json.dumps({"fix_status": fix}).encode(), stamp)
        writer.add_message(other_channel, START + 9, b'{"value":1}', START + 9)
        writer.finish()


def main() -> None:
    root = Path(sys.argv[1])
    root.mkdir(parents=True, exist_ok=True)
    if list(root.iterdir()):
        raise ValueError("synthetic input directory must be empty")
    write_recording(root / "legacy.mcap", "OBSTACLE_STOP")
    write_recording(root / "nested" / "prefixed.mcap", "STATE_OBSTACLE_STOP")
    print("FIXTURE PASS: two synthetic MCAPs, 20 records, both state aliases, nested key", flush=True)


if __name__ == "__main__":
    main()
