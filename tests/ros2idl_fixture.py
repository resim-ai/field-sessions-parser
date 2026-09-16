"""A ROS 2 MCAP whose schemas are IDL in rosbag2's concatenated form: 2 Hz status with a byte blob."""

from __future__ import annotations

from pathlib import Path

import numpy as np
from conftest import NS, T0
from mcap.writer import Writer
from rosbags.typesys import Stores, get_types_from_idl, get_typestore

STATUS_HZ = 2
SEPARATOR = "=" * 80

NESTED_IDL = """\
module demo_msgs {
  module msg {
    struct Nested {
      float gain;
      sequence<uint8> blob;
    };
  };
};
"""

STATUS_IDL = """\
#include "demo_msgs/msg/Nested.idl"

module demo_msgs {
  module msg {
    typedef double double__4[4];
    @verbatim (language="comment", text="Robot status")
    struct Status {
      double battery_v;
      int32 mode;
      boolean armed;
      string label;
      demo_msgs::msg::Nested nested;
      double__4 covariance;
    };
  };
};
"""

STATUS_SCHEMA = (
    f"{SEPARATOR}\nIDL: demo_msgs/msg/Status\n{STATUS_IDL}\n"
    f"{SEPARATOR}\nIDL: demo_msgs/msg/Nested\n{NESTED_IDL}"
)

ROS2 = get_typestore(Stores.ROS2_HUMBLE)
ROS2.register(get_types_from_idl(STATUS_SCHEMA))


def status_message(step: int):
    types = ROS2.types
    nested = types["demo_msgs/msg/Nested"](gain=0.25 * step, blob=np.array([step % 256, 7], dtype=np.uint8))
    return types["demo_msgs/msg/Status"](
        battery_v=48.0 - 0.1 * step,
        mode=step % 3,
        armed=step % 2 == 0,
        label=f"leg-{step}",
        nested=nested,
        covariance=np.zeros(4),
    )


def build_ros2idl_mcap(path: Path, *, seconds: float = 6.0, chunk_size: int = 1024) -> Path:
    with open(path, "wb") as handle:
        writer = Writer(handle, chunk_size=chunk_size)
        writer.start(profile="ros2", library="field-sessions-fixture")
        schema = writer.register_schema("demo_msgs/msg/Status", "ros2idl", STATUS_SCHEMA.encode())
        channel = writer.register_channel("/robot/status", "cdr", schema)
        for step in range(int(seconds * STATUS_HZ)):
            t_ns = T0 + step * NS // STATUS_HZ
            data = ROS2.serialize_cdr(status_message(step), "demo_msgs/msg/Status")
            writer.add_message(channel, t_ns, data, t_ns, step)
        writer.finish()
    return path


ROS2IDL_PLAN = {
    "channels": [
        {
            "topic": "/robot/status",
            "fields": [
                {"path": "battery_v", "type": "double"},
                {"path": "mode", "type": "bigint"},
                {"path": "armed", "type": "boolean"},
                {"path": "label", "type": "string"},
                {"path": "nested.gain", "column": "gain", "type": "double"},
            ],
        }
    ],
    "events": [
        {
            "name": "low_battery",
            "channel": "/robot/status",
            "field": "battery_v",
            "below": 47.5,
            "held_for_s": 1.0,
            "status": "FAIL_BLOCK",
            "tags": ["battery"],
            "description": "Battery under 47.5 V",
        }
    ],
}
