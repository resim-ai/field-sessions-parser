from __future__ import annotations

import json
import struct
import sys

import pytest
from mcap.writer import Writer

from field_sessions_parser import inspect, open
from field_sessions_parser.cli import main
from field_sessions_parser.logs import epoch_ns
from field_sessions_parser.reader import LogReader
from field_sessions_parser.remote import LocalRangeFile


def test_mcap_provenance_and_decoder_failures(ros1_mcap, ros1_mixed_mcap):
    rows = list(open(ros1_mcap, recording_key="nested/drive.mcap").iter_messages({"/vehicle/odom"}))
    assert len(rows) == 60
    assert all(r.recording_key == "nested/drive.mcap" for r in rows)
    with pytest.raises(Exception, match="cannot decode"):
        inspect(ros1_mixed_mcap)


def test_ros_inspection_includes_nested_fields(ros1_mcap, ros2idl_mcap):
    assert "twist.twist.linear.x" in inspect(ros1_mcap)["fields"]["/vehicle/odom"]
    report = inspect(ros2idl_mcap)
    assert report["topics"]
    assert all(report["fields"][topic] for topic in report["topics"])


def test_corrupted_mcap_checksum_cannot_report_complete(ros1_mcap, tmp_path):
    with LocalRangeFile(ros1_mcap) as stream:
        reader = LogReader(stream).open()
        crc_offset = reader.chunk_indexes[0].chunk_start_offset + 9 + 24
    data = bytearray(ros1_mcap.read_bytes())
    original = struct.unpack_from("<I", data, crc_offset)[0]
    struct.pack_into("<I", data, crc_offset, (original ^ 0xFFFFFFFF) or 1)
    path = tmp_path / "corrupt.mcap"
    path.write_bytes(data)
    with pytest.raises(Exception, match="incomplete recording"):
        inspect(path)


@pytest.mark.parametrize("value", [True, 1.0, -1, "NaN", "1.1", "infinity"])
def test_invalid_nanosecond_clock_rejected(value):
    with pytest.raises(ValueError):
        epoch_ns(value)


@pytest.mark.parametrize("suffix", [".log", ".txt", ".jsonl", ".parquet", ".h5", ".hdf5", ".bag", ".unknown"])
def test_non_mcap_format_rejected_before_read(tmp_path, suffix):
    with pytest.raises(ValueError, match="unsupported recording format"):
        open(tmp_path / f"recording{suffix}")


def test_cli_inspects_mcap_with_exact_bounds(tmp_path, monkeypatch, capsys):
    path = tmp_path / "drive.mcap"
    start = 1789420800000000001
    with path.open("wb") as output:
        writer = Writer(output)
        writer.start()
        schema = writer.register_schema("telemetry", "jsonschema", b'{"type":"object"}')
        channel = writer.register_channel("/speed", "json", schema)
        for stamp in [start, start + 1]:
            writer.add_message(channel, stamp, b'{"speed":2}', stamp)
        writer.finish()
    monkeypatch.setattr(sys, "argv", ["field-sessions-parser", "inspect", str(path)])
    assert main() == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert json.loads(output.out) == {
        "recording_key": "drive.mcap",
        "format": "mcap",
        "message_count": 2,
        "topics": {"/speed": 2},
        "fields": {"/speed": ["speed"]},
        "start_ns": start,
        "end_ns": start + 1,
        "unclocked_messages": 0,
        "complete": True,
    }


def test_cli_unsupported_format_fails_without_read(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["field-sessions-parser", "inspect", str(tmp_path / "drive.bag")])
    assert main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    failure = json.loads(output.err)
    assert failure["complete"] is False
    assert "unsupported recording format" in failure["error"]


def test_summary_reads_metadata_without_message_traversal(ros1_mcap, monkeypatch):
    import io

    from field_sessions_parser import logs, summarize

    with LocalRangeFile(ros1_mcap) as stream:
        reader = LogReader(stream).open()
        chunks = [(c.chunk_start_offset, c.chunk_start_offset + c.chunk_length) for c in reader.chunk_indexes]

    class MetadataOnlyStream(io.BytesIO):
        def read(self, size=-1):
            assert size >= 0, "summary requested an unbounded read"
            start = self.tell()
            assert not any(start < end and start + size > begin for begin, end in chunks), (
                "summary traversed a message chunk"
            )
            return super().read(size)

    def forbid_messages(*args, **kwargs):
        pytest.fail("metadata discovery must not iterate or decode messages")

    monkeypatch.setattr(logs, "open_source", lambda *a, **kw: MetadataOnlyStream(ros1_mcap.read_bytes()))
    monkeypatch.setattr(LogReader, "iter_messages", forbid_messages)
    monkeypatch.setattr(LogReader, "decode", forbid_messages)
    report = summarize(ros1_mcap, recording_key="nested/drive.mcap")
    assert report["summary_only"] is True
    assert report["recording_key"] == "nested/drive.mcap"
    assert report["header"] == {"profile": "ros1", "library": "field-sessions-fixture"}
    assert report["statistics"]["message_count"] == 138
    assert {c["topic"]: c["message_count"] for c in report["channels"]}["/vehicle/odom"] == 60
    schema = next(s for s in report["schemas"] if s["name"] == "demo_msgs/Status")
    assert schema["data_encoding"] == "utf8"
    assert "int32 mode" in schema["data"]
    assert "fields" not in report and "complete" not in report


@pytest.mark.parametrize("statistics", [False, True])
def test_summary_reports_recorded_statistics_without_decoding(tmp_path, statistics):
    import base64

    from field_sessions_parser import summarize

    path = tmp_path / "unknown.mcap"
    start = 1789420800000000001
    with path.open("wb") as output:
        writer = Writer(output, use_statistics=statistics)
        writer.start()
        schema = writer.register_schema("opaque", "unknown", b"\xff\x00")
        channel = writer.register_channel("/state", "unknown", schema, metadata={"source": "synthetic"})
        for timestamp in [start, start + 1]:
            writer.add_message(channel, timestamp, b"undecodable", timestamp)
        writer.finish()
    report = summarize(path)
    assert report["schemas"][0]["data_encoding"] == "base64"
    assert base64.b64decode(report["schemas"][0]["data"]) == b"\xff\x00"
    assert report["channels"][0]["metadata"] == {"source": "synthetic"}
    assert report["channels"][0]["message_count"] == (2 if statistics else None)
    assert report["statistics"] == (
        {"message_count": 2, "start_ns": start, "end_ns": start + 1} if statistics else None
    )
    with pytest.raises(Exception, match="cannot decode"):
        inspect(path)


def test_summary_cli_and_missing_summary_do_not_scan(ros1_mcap, no_summary_mcap, monkeypatch, capsys):
    from field_sessions_parser.logs import Recording

    monkeypatch.setattr(Recording, "iter_messages", lambda *a, **kw: pytest.fail("full scan fallback"))
    monkeypatch.setattr(sys, "argv", ["field-sessions-parser", "summary", str(ros1_mcap)])
    assert main() == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert json.loads(output.out)["summary_only"] is True
    monkeypatch.setattr(sys, "argv", ["field-sessions-parser", "summary", str(no_summary_mcap)])
    assert main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "no summary section" in json.loads(output.err)["error"]
