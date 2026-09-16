from __future__ import annotations

import argparse
import os
from collections import Counter
from pathlib import Path

from field_sessions_parser import logs
from field_sessions_parser.emissions import emit_event, emit_session


def field(data, path):
    for key in path.split("."):
        data = data[key] if isinstance(data, dict) else getattr(data, key)
    return data


def obstacle_stop_mask(data):
    masks = []
    for name in ("OBSTACLE_STOP", "STATE_OBSTACLE_STOP"):
        try:
            masks.append(field(data, name))
        except (AttributeError, KeyError):
            continue
    if not masks:
        raise ValueError("missing recorded obstacle-stop bit field definition")
    if any(type(mask) is not int or mask <= 0 or mask & (mask - 1) for mask in masks):
        raise ValueError("invalid recorded obstacle-stop bit field definition")
    if len(set(masks)) != 1:
        raise ValueError("conflicting recorded obstacle-stop bit field definitions")
    return masks[0]


def burro_observation(record, key, emitter, previous):
    data, stamp = record.data, record.timestamp_ns
    if record.topic == "/unicycle/unicycle_node/state":
        state = field(data, "state")
        mask = obstacle_stop_mask(data)
        if type(state) is not int or state < 0:
            raise ValueError("invalid recorded obstacle-stop bit field")
        active = bool(state & mask)
        observed = active and not previous.get("obstacle_stop", False)
        emitter.emit(
            "burro_state",
            {"recording_key": key, "state": state, "obstacle_stop": active, "stop_observation": observed},
            timestamp=stamp,
        )
        if observed:
            emit_event(
                emitter,
                recording_key=key,
                timestamp_ns=stamp,
                name="Obstacle stop observed",
                description=(
                    "The recorded obstacle-stop state bit is set; "
                    "first observation or reappearance within this file."
                ),
                tags=["obstacle-stop"],
            )
        previous["obstacle_stop"] = active
    elif record.topic == "/gps_status":
        fix = field(data, "fix_status")
        if not isinstance(fix, str) or not fix:
            raise ValueError("invalid recorded GPS fix status")
        has_previous = "fix_status" in previous
        changed = has_previous and previous["fix_status"] != fix
        emitter.emit(
            "burro_gps",
            {"recording_key": key, "fix_status": fix, "has_previous": has_previous, "fix_changed": changed},
            timestamp=stamp,
        )
        if changed:
            emit_event(
                emitter,
                recording_key=key,
                timestamp_ns=stamp,
                name="GPS fix changed",
                description=f"Recorded fix status changed from {previous['fix_status']} to {fix}.",
                tags=["gps-fix"],
            )
        previous["fix_status"] = fix


def analyze_recording(path, root, emitter, profile):
    key = path.relative_to(root).as_posix()
    counts = Counter()
    start = end = None
    burro_previous = {}
    previous_stamp = None
    for record in logs.open(path, recording_key=key).iter_messages():
        stamp = record.timestamp_ns
        if stamp is None:
            raise ValueError(f"{key}: missing clock; explicit timestamp mapping is required")
        if previous_stamp is not None and stamp < previous_stamp:
            raise ValueError(f"{key}: messages are not ordered by time")
        previous_stamp = stamp
        counts[record.topic] += 1
        start = stamp if start is None else min(start, stamp)
        end = stamp if end is None else max(end, stamp)
        if profile == "burro":
            burro_observation(record, key, emitter, burro_previous)

    if start is None:
        raise ValueError(f"{key}: recording has no clocked messages")
    for topic, count in sorted(counts.items()):
        emitter.emit(
            "recording_topics",
            {"recording_key": key, "topic_name": topic, "message_count": count},
            timestamp=start,
        )
    emit_event(
        emitter,
        recording_key=key,
        timestamp_ns=start,
        name="Recording started",
        description="First observed message in this recording; an inventory marker, not a detected fault.",
        tags=["recording"],
    )
    return start, end


def run(folder, emitter, *, profile="inventory"):
    if profile not in {"inventory", "burro"}:
        raise ValueError("unknown robot profile")
    root = Path(folder).resolve(strict=True)
    paths = sorted(root.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise ValueError("session contains a symlink; provide regular recording files")
    files = [path for path in paths if path.is_file()]
    if not files:
        raise ValueError("session folder contains no recordings")
    for path in files:
        if not path.resolve().is_relative_to(root):
            raise ValueError("recording symlink escapes the session folder")
        if path.suffix.lower() != ".mcap":
            raise ValueError(f"unsupported input file: {path.relative_to(root)}")
    bounds = [analyze_recording(path, root, emitter, profile) for path in files]
    emit_session(emitter, root, start_ns=min(b[0] for b in bounds), end_ns=max(b[1] for b in bounds))


def write_output(folder, output, config, profile, emitter_factory):
    output = Path(output)
    if output.resolve().is_relative_to(Path(folder).resolve()):
        raise ValueError("output must be outside the input folder")
    output.parent.mkdir(parents=True, exist_ok=True)
    pending = output.with_name(output.name + ".pending")
    output.unlink(missing_ok=True)
    try:
        emitter = emitter_factory(config_path=str(config), output_path=str(pending))
        try:
            run(folder, emitter, profile=profile)
        finally:
            emitter.close()
        pending.replace(output)
    finally:
        pending.unlink(missing_ok=True)


def main():
    from resim.sdk.metrics.emissions import Emitter

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--robot-profile",
        choices=["inventory", "burro"],
        default=os.environ.get("SESSION_ROBOT_PROFILE", "inventory"),
    )
    parser.add_argument("--input", default="/tmp/resim/inputs/experience")
    parser.add_argument("--output", default="/tmp/resim/outputs/emissions.resim.jsonl")
    parser.add_argument("--config", default=str(Path(__file__).with_name("config.resim.yml")))
    args = parser.parse_args()
    write_output(args.input, args.output, args.config, args.robot_profile, Emitter)


if __name__ == "__main__":
    main()
