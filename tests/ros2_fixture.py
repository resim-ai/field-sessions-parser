"""A rosbag2-shaped ROS 2 MCAP: CDR odometry plus one channel whose schema the typestore rejects."""

from __future__ import annotations

from pathlib import Path

import numpy as np
from conftest import NS, T0, speed_at
from mcap.writer import Writer
from rosbags.typesys import Stores, get_typestore

ROS2 = get_typestore(Stores.ROS2_HUMBLE)

BROKEN_SCHEMA = "float64 speed\nmystery_pkg/Thing thing\n"


def ros2_odometry(t_s: float, distance: float):
    types = ROS2.types
    stamp = types["builtin_interfaces/msg/Time"](sec=int(T0 // NS + int(t_s)), nanosec=int((t_s % 1) * NS))
    header = types["std_msgs/msg/Header"](stamp=stamp, frame_id="odom")
    point = types["geometry_msgs/msg/Point"](x=distance, y=0.0, z=0.0)
    quat = types["geometry_msgs/msg/Quaternion"](x=0.0, y=0.0, z=0.0, w=1.0)
    pose = types["geometry_msgs/msg/PoseWithCovariance"](
        pose=types["geometry_msgs/msg/Pose"](position=point, orientation=quat), covariance=np.zeros(36)
    )
    twist = types["geometry_msgs/msg/TwistWithCovariance"](
        twist=types["geometry_msgs/msg/Twist"](
            linear=types["geometry_msgs/msg/Vector3"](x=speed_at(t_s), y=0.0, z=0.0),
            angular=types["geometry_msgs/msg/Vector3"](x=0.0, y=0.0, z=0.0),
        ),
        covariance=np.zeros(36),
    )
    return types["nav_msgs/msg/Odometry"](header=header, child_frame_id="base_link", pose=pose, twist=twist)


def build_ros2_mcap(path: Path, *, seconds: float = 6.0, hz: int = 10, chunk_size: int = 2048) -> Path:
    with open(path, "wb") as handle:
        writer = Writer(handle, chunk_size=chunk_size)
        writer.start(profile="ros2", library="field-sessions-fixture")
        odom_schema = writer.register_schema(
            "nav_msgs/msg/Odometry",
            "ros2msg",
            ROS2.generate_msgdef("nav_msgs/msg/Odometry", ros_version=2)[0].encode(),
        )
        broken_schema = writer.register_schema("demo_msgs/msg/Broken", "ros2msg", BROKEN_SCHEMA.encode())
        odom_channel = writer.register_channel("/odom", "cdr", odom_schema)
        broken_channel = writer.register_channel("/broken", "cdr", broken_schema)
        distance = 0.0
        for step in range(int(seconds * hz)):
            t_s = step / hz
            t_ns = T0 + int(round(t_s * NS))
            distance += speed_at(t_s) / hz
            data = ROS2.serialize_cdr(ros2_odometry(t_s, distance), "nav_msgs/msg/Odometry")
            writer.add_message(odom_channel, t_ns, data, t_ns, step)
            if step == 0:
                writer.add_message(broken_channel, t_ns, b"\x00\x01\x00\x00" + bytes(16), t_ns, 0)
        writer.finish()
    return path
