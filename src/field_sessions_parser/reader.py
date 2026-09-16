"""MCAP reading over a range file: footer and summary first, then chunks only when asked for."""

from __future__ import annotations

import heapq
import json
import math
import zlib
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import lz4.frame
import zstandard
from google.protobuf import (
    any_pb2,
    api_pb2,
    descriptor_pb2,
    descriptor_pool,
    duration_pb2,
    empty_pb2,
    field_mask_pb2,
    message_factory,
    source_context_pb2,
    struct_pb2,
    timestamp_pb2,
    type_pb2,
    wrappers_pb2,
)
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import DecodeError
from mcap.data_stream import ReadDataStream
from mcap.exceptions import McapError
from mcap.opcode import Opcode
from mcap.reader import FOOTER_SIZE
from mcap.records import (
    AttachmentIndex,
    Channel,
    Chunk,
    ChunkIndex,
    Footer,
    Header,
    Message,
    MetadataIndex,
    Schema,
    Statistics,
)
from mcap.stream_reader import MAGIC_SIZE, StreamReader, breakup_chunk
from mcap.summary import Summary
from rosbags.typesys import Stores, TypesysError, get_types_from_idl, get_types_from_msg, get_typestore
from rosbags.typesys.msg import normalize_msgtype

from .remote import SourceError

MCAP_MAGIC = bytes((137, 77, 67, 65, 80, 48, 13, 10))
MAX_CHUNK_UNCOMPRESSED = 256 * 1024 * 1024
MAX_SUMMARY_BYTES = 256 * 1024 * 1024
RECORD_PREFIX = 1 + 8

Decoder = Callable[[bytes], Any]
UNDECODABLE = object()

SCHEMA_ENCODINGS: dict[str, tuple[str, ...]] = {
    "ros1": ("ros1msg",),
    "cdr": ("ros2msg", "ros2idl"),
    "protobuf": ("protobuf",),
}

# The `google/protobuf/*.proto` files every protobuf runtime ships, keyed by file name: a set
# written without `--include_imports` imports them without embedding them.
WELL_KNOWN_FILES: dict[str, Any] = {
    module.DESCRIPTOR.name: module.DESCRIPTOR
    for module in (
        any_pb2,
        api_pb2,
        descriptor_pb2,
        duration_pb2,
        empty_pb2,
        field_mask_pb2,
        source_context_pb2,
        struct_pb2,
        timestamp_pb2,
        type_pb2,
        wrappers_pb2,
    )
}


@dataclass(frozen=True)
class BuiltDecoder:
    """The built-in that owns a channel; `decode` is None when the channel's schema failed to register."""

    name: str
    decode: Decoder | None


class LogError(Exception):
    """The object cannot be read as an indexed MCAP."""


class NotMcapError(LogError):
    pass


class NoSummaryError(LogError):
    pass


class LogReader:
    def __init__(
        self,
        stream,
        *,
        max_chunk_uncompressed: int = MAX_CHUNK_UNCOMPRESSED,
        max_summary_bytes: int = MAX_SUMMARY_BYTES,
    ):
        self._stream = stream
        self._max_chunk_uncompressed = max_chunk_uncompressed
        self._max_summary_bytes = max_summary_bytes
        self.header = Header(profile="", library="")
        self.summary = Summary()
        self.chunk_indexes: list[ChunkIndex] = []
        self.decode_failures: dict[str, int] = {}
        self.schema_failures: dict[str, str] = {}
        self.chunks_read = 0
        self._chunked = False
        self._stores: dict[str, Any] = {}
        self._decoders: dict[int, BuiltDecoder | None] = {}
        self._registered: dict[tuple[str, int], bool] = {}
        self._message_classes: dict[int, type | str] = {}

    def open(self) -> LogReader:
        stream = self._stream
        stream.seek(0, 2)
        size = stream.tell()
        if size < 2 * MAGIC_SIZE + FOOTER_SIZE:
            raise NotMcapError(f"object is {size} bytes, too small to be an MCAP")
        stream.seek(size - MAGIC_SIZE)
        if stream.read(MAGIC_SIZE) != MCAP_MAGIC:
            raise NotMcapError("not an MCAP file: the trailing magic is missing")
        stream.seek(size - MAGIC_SIZE - FOOTER_SIZE)
        data = ReadDataStream(stream)
        if data.read1() != Opcode.FOOTER:
            raise NotMcapError("not an MCAP file: no footer record before the trailing magic")
        data.read8()
        footer = Footer.read(data)
        if footer.summary_start == 0:
            raise NoSummaryError(
                "the MCAP has no summary section; run `mcap recover` on the file and upload the result"
            )
        summary_bytes = size - footer.summary_start
        if summary_bytes > self._max_summary_bytes:
            raise NoSummaryError(
                f"the MCAP footer points at a {summary_bytes} byte summary section, over the "
                f"{self._max_summary_bytes} byte cap; run `mcap recover` on the file"
            )
        stream.seek(footer.summary_start)
        try:
            self.summary = _read_summary(
                StreamReader(
                    stream, skip_magic=True, emit_chunks=True, record_size_limit=self._max_summary_bytes
                )
            )
        except SourceError:
            raise
        except Exception as err:
            raise NoSummaryError(
                f"the MCAP summary section is unreadable ({err}); run `mcap recover`"
            ) from err
        if not self.summary.channels:
            raise NoSummaryError("the MCAP summary lists no channels; run `mcap recover` on the file")

        stream.seek(0)
        if stream.read(MAGIC_SIZE) != MCAP_MAGIC:
            raise NotMcapError("not an MCAP file: the leading magic is missing")
        data = ReadDataStream(stream)
        if data.read1() == Opcode.HEADER:
            data.read8()
            self.header = Header.read(data)

        ordered = sorted(
            self.summary.chunk_indexes, key=lambda ci: (ci.message_start_time, ci.chunk_start_offset)
        )
        self._chunked = bool(ordered)
        for chunk_index in ordered:
            oversize = self._oversize_reason(chunk_index)
            if oversize is not None:
                self._fail_chunk(chunk_index, oversize)
            else:
                self.chunk_indexes.append(chunk_index)
        return self

    @property
    def channels(self) -> dict[int, Channel]:
        return self.summary.channels

    @property
    def schemas(self) -> dict[int, Schema]:
        return self.summary.schemas

    @property
    def statistics(self) -> Statistics | None:
        return self.summary.statistics

    @property
    def indexed(self) -> bool:
        return any(ci.message_index_offsets for ci in self.summary.chunk_indexes)

    def schema_for(self, channel: Channel) -> Schema | None:
        return self.schemas.get(channel.schema_id) if channel.schema_id else None

    def decoder_for(self, channel: Channel) -> Decoder | None:
        built = self._built(channel)
        return built.decode if built is not None else None

    def decoder_name(self, channel: Channel) -> str | None:
        """`builtin:<name>` of the decoder that reads the channel, None when nothing does."""
        built = self._built(channel)
        return built.name if built is not None and built.decode is not None else None

    def encoding_supported(self, channel: Channel) -> bool:
        """False when no built-in owns the channel's encoding, so no message of it ever decodes."""
        return self._built(channel) is not None

    def _built(self, channel: Channel) -> BuiltDecoder | None:
        if channel.id not in self._decoders:
            self._decoders[channel.id] = self._build_decoder(channel)
        return self._decoders[channel.id]

    def decode(self, channel: Channel, message: Message) -> Any:
        decoder = self.decoder_for(channel)
        if decoder is None:
            self.note_decode_failure(channel.topic)
            return UNDECODABLE
        try:
            return decoder(message.data)
        except Exception:
            self.note_decode_failure(channel.topic)
            return UNDECODABLE

    def note_decode_failure(self, key: str, count: int = 1) -> None:
        self.decode_failures[key] = self.decode_failures.get(key, 0) + count

    def read_chunk(self, chunk_index: ChunkIndex) -> list[Message]:
        self.chunks_read += 1
        try:
            return _chunk_messages(self._read_chunk_record(chunk_index))
        except SourceError:
            raise
        except Exception as err:
            self._fail_chunk(
                chunk_index, f"chunk at {chunk_index.chunk_start_offset} failed to decode: {err}"
            )
            return []

    def _read_chunk_record(self, chunk_index: ChunkIndex) -> Chunk:
        """Chunk.read trusts the record's own lengths; every length is checked against the index first."""
        self._stream.seek(chunk_index.chunk_start_offset)
        data = ReadDataStream(self._stream)
        opcode, length = data.read1(), data.read8()
        if opcode != Opcode.CHUNK or length != chunk_index.chunk_length - RECORD_PREFIX:
            raise McapError("the chunk record header disagrees with its chunk index")
        message_start_time = data.read8()
        message_end_time = data.read8()
        uncompressed_size = data.read8()
        uncompressed_crc = data.read4()
        compression = chunk_index.compression.encode()
        compression_length = data.read4()
        if compression_length != len(compression) or data.read(compression_length) != compression:
            raise McapError("the chunk record compression disagrees with its chunk index")
        data_length = data.read8()
        if uncompressed_size != chunk_index.uncompressed_size or data_length != chunk_index.compressed_size:
            raise McapError(
                f"the chunk record declares {data_length} compressed and {uncompressed_size} uncompressed "
                f"bytes, its index {chunk_index.compressed_size} and {chunk_index.uncompressed_size}"
            )
        return Chunk(
            compression=chunk_index.compression,
            data=data.read(data_length),
            message_end_time=message_end_time,
            message_start_time=message_start_time,
            uncompressed_crc=uncompressed_crc,
            uncompressed_size=uncompressed_size,
        )

    def window_chunks(self, topics: Iterable[str] | None = None) -> list[ChunkIndex]:
        """The first and the last indexed chunk holding a wanted channel: two windows per file at most."""
        if not self.indexed:
            return []
        wanted = None if topics is None else set(topics)
        ids = {cid for cid, channel in self.channels.items() if wanted is None or channel.topic in wanted}
        holding = [ci for ci in self.chunk_indexes if ids & ci.message_index_offsets.keys()]
        if not holding:
            return []
        if holding[0] is holding[-1]:
            return [holding[0]]
        return [holding[0], holding[-1]]

    def channel_chunk_bounds(self) -> dict[int, tuple[ChunkIndex, ChunkIndex]]:
        """The first and the last indexed chunk holding each channel."""
        bounds: dict[int, tuple[ChunkIndex, ChunkIndex]] = {}
        for chunk_index in self.chunk_indexes:
            for cid in chunk_index.message_index_offsets:
                first, _ = bounds.get(cid, (chunk_index, chunk_index))
                bounds[cid] = (first, chunk_index)
        return bounds

    def last_chunk_per_channel(self, windows: list[ChunkIndex]) -> dict[int, ChunkIndex]:
        last: dict[int, ChunkIndex] = {}
        for chunk_index in windows:
            for cid in chunk_index.message_index_offsets:
                last[cid] = chunk_index
        return last

    def iter_messages(
        self, topics: Iterable[str] | None = None, log_time_order: bool = True
    ) -> Iterator[tuple[Channel, Message]]:
        channels = self.channels
        wanted_ids: set[int] | None = None
        if topics is not None:
            wanted = set(topics)
            wanted_ids = {cid for cid, channel in channels.items() if channel.topic in wanted}
            if not wanted_ids:
                return
        if not self.chunk_indexes:
            if not self._chunked:
                yield from self._iter_linear(wanted_ids)
            return
        candidates = [
            ci
            for ci in self.chunk_indexes
            if wanted_ids is None
            or not ci.message_index_offsets
            or wanted_ids & ci.message_index_offsets.keys()
        ]
        heap: list[tuple[int, int, int, Message]] = []
        for position, chunk_index in enumerate(candidates):
            for index, message in enumerate(self.read_chunk(chunk_index)):
                if wanted_ids is not None and message.channel_id not in wanted_ids:
                    continue
                if message.channel_id not in channels:
                    self.note_decode_failure(f"channel#{message.channel_id}")
                    continue
                if log_time_order:
                    heapq.heappush(heap, (message.log_time, chunk_index.chunk_start_offset, index, message))
                else:
                    yield channels[message.channel_id], message
            if log_time_order:
                next_start = (
                    candidates[position + 1].message_start_time if position + 1 < len(candidates) else None
                )
                while heap and (next_start is None or heap[0][0] < next_start):
                    message = heapq.heappop(heap)[3]
                    yield channels[message.channel_id], message
        while heap:
            message = heapq.heappop(heap)[3]
            yield channels[message.channel_id], message

    def _iter_linear(self, wanted_ids: set[int] | None) -> Iterator[tuple[Channel, Message]]:
        self._stream.seek(0)
        channels = self.channels
        for record in StreamReader(self._stream).records:
            if not isinstance(record, Message):
                continue
            if wanted_ids is not None and record.channel_id not in wanted_ids:
                continue
            channel = channels.get(record.channel_id)
            if channel is None:
                self.note_decode_failure(f"channel#{record.channel_id}")
                continue
            yield channel, record

    def _oversize_reason(self, chunk_index: ChunkIndex) -> str | None:
        cap = self._max_chunk_uncompressed
        where = f"chunk at {chunk_index.chunk_start_offset}"
        if chunk_index.uncompressed_size > cap:
            return (
                f"{where} declares {chunk_index.uncompressed_size} uncompressed bytes, "
                f"over the {cap} byte cap"
            )
        if chunk_index.chunk_length > cap:
            return f"{where} declares a {chunk_index.chunk_length} byte record, over the {cap} byte cap"
        return None

    def _fail_chunk(self, chunk_index: ChunkIndex, reason: str) -> None:
        keys = [self.channels[cid].topic for cid in chunk_index.message_index_offsets if cid in self.channels]
        if not keys:
            keys = [f"chunk@{chunk_index.chunk_start_offset}"]
        for key in keys:
            self.note_decode_failure(key)
        self.schema_failures.setdefault(f"chunk@{chunk_index.chunk_start_offset}", reason)

    def _store(self, profile: str):
        if profile not in self._stores:
            store_name = Stores.ROS1_NOETIC if profile == "ros1" else Stores.ROS2_HUMBLE
            self._stores[profile] = get_typestore(store_name)
        return self._stores[profile]

    def _build_decoder(self, channel: Channel) -> BuiltDecoder | None:
        encoding = channel.message_encoding
        schema = self.schema_for(channel)
        if encoding == "json":
            return BuiltDecoder("builtin:json", lambda data: json.loads(data))
        expected = SCHEMA_ENCODINGS.get(encoding)
        if expected is None:
            self.schema_failures[channel.topic] = f"unsupported encoding {encoding!r}"
            return None
        if encoding == "protobuf":
            return self._protobuf_decoder(channel, schema)
        name = f"builtin:{encoding}"
        if schema is None or schema.encoding not in expected:
            found = schema.encoding if schema else "none"
            self.schema_failures[channel.topic] = (
                f"{encoding} messages need a {' or '.join(expected)} schema, found {found!r}"
            )
            return BuiltDecoder(name, None)
        if schema.encoding == "ros2idl":
            name = "builtin:ros2idl"
        profile = "ros1" if encoding == "ros1" else "ros2"
        store = self._store(profile)
        typename = normalize_msgtype(schema.name)
        if not self._register(profile, store, schema, typename):
            return BuiltDecoder(name, None)
        if encoding == "ros1":
            return BuiltDecoder(name, lambda data: store.deserialize_ros1(data, typename))
        return BuiltDecoder(name, lambda data: store.deserialize_cdr(data, typename))

    def _protobuf_decoder(self, channel: Channel, schema: Schema | None) -> BuiltDecoder | None:
        """A protobuf channel is owned only when its embedded FileDescriptorSet yields its message class."""
        if schema is None or schema.encoding != "protobuf":
            found = schema.encoding if schema else "none"
            self.schema_failures[channel.topic] = (
                f"unsupported encoding 'protobuf': no protobuf FileDescriptorSet schema (found {found!r})"
            )
            return None
        if schema.id not in self._message_classes:
            self._message_classes[schema.id] = _protobuf_message_class(schema)
        message_class = self._message_classes[schema.id]
        if isinstance(message_class, str):
            self.schema_failures[channel.topic] = f"unsupported encoding 'protobuf': {message_class}"
            return None
        return BuiltDecoder("builtin:protobuf", lambda data: protobuf_tree(message_class.FromString(data)))

    def _register(self, profile: str, store, schema: Schema, typename: str) -> bool:
        key = (profile, schema.id)
        if key in self._registered:
            return self._registered[key]
        self._registered[key] = False
        text = schema.data.decode("utf-8", errors="replace")
        try:
            if schema.encoding == "ros2idl":
                types = get_types_from_idl(text)
            else:
                types = get_types_from_msg(text, typename)
        except Exception as err:
            self.schema_failures[schema.name] = f"schema text does not parse: {err}"
            return False
        # Rosbags compares serialized fields only when reusing an existing message class.
        for name, (constants, _) in types.items():
            if len({constant[0] for constant in constants}) != len(constants):
                self.schema_failures[schema.name] = f"duplicate constants in type {name}"
                return False
            if name in store.fielddefs and not _same_ros_constants(constants, store.fielddefs[name][0]):
                self.schema_failures[schema.name] = f"type {name} already present with different constants"
                return False
        try:
            store.register(types)
        except TypesysError as err:
            self.schema_failures[schema.name] = f"schema cannot be registered: {err}"
            return False
        if typename not in store.types:
            self.schema_failures[schema.name] = f"type {typename} could not be registered"
            return False
        try:
            store.get_msgdef(typename)
        except Exception as err:
            self.schema_failures[schema.name] = f"schema references a type it does not define: {err}"
            return False
        self._registered[key] = True
        return True


def _same_ros_constants(left, right):
    left = {name: (kind, value) for name, kind, value in left}
    right = {name: (kind, value) for name, kind, value in right}
    if left.keys() != right.keys():
        return False
    for name, (kind, value) in left.items():
        other_kind, other_value = right[name]
        if kind != other_kind or type(value) is not type(other_value):
            return False
        both_nan = isinstance(value, float) and math.isnan(value) and math.isnan(other_value)
        if value != other_value and not both_nan:
            return False
    return True


def _protobuf_message_class(schema: Schema) -> type | str:
    """The class the schema's FileDescriptorSet defines under the schema's name, or the reason it does not."""
    try:
        files = descriptor_pb2.FileDescriptorSet.FromString(schema.data).file
    except DecodeError as err:
        return f"the schema is not a FileDescriptorSet ({err})"
    pool = descriptor_pool.DescriptorPool()
    try:
        _load_descriptor_files(pool, files)
    except Exception as err:
        return f"the FileDescriptorSet cannot be loaded ({err})"
    try:
        descriptor = pool.FindMessageTypeByName(schema.name)
    except KeyError:
        return f"the FileDescriptorSet does not define {schema.name!r}"
    return message_factory.GetMessageClass(descriptor)


def _load_descriptor_files(pool: descriptor_pool.DescriptorPool, files: Iterable[Any]) -> None:
    """Files enter the pool after their dependencies whatever order the set lists them in; a
    well-known file the set imports but omits comes from the runtime, anything else missing
    fails in `AddSerializedFile` naming it."""
    pending = {file.name: file for file in files}
    loaded: set[str] = set()
    while pending:
        ready = [
            name
            for name, file in pending.items()
            if all(dep in loaded or dep not in pending for dep in file.dependency)
        ]
        for name in ready or [next(iter(pending))]:
            file = pending.pop(name)
            for dep in file.dependency:
                if dep not in loaded and dep not in pending:
                    _load_well_known_file(pool, dep, loaded)
            pool.AddSerializedFile(file.SerializeToString())
            loaded.add(name)


def _load_well_known_file(pool: descriptor_pool.DescriptorPool, name: str, loaded: set[str]) -> None:
    known = WELL_KNOWN_FILES.get(name)
    if known is None:
        return
    for dep in known.dependencies:
        if dep.name not in loaded:
            _load_well_known_file(pool, dep.name, loaded)
    pool.AddSerializedFile(known.serialized_pb)
    loaded.add(name)


def protobuf_tree(message: Any) -> dict[str, Any]:
    """Every declared field in declaration order: enums by name, bytes kept as bytes, maps as dicts,
    unset sub-messages as None so a recursive type ends."""
    tree: dict[str, Any] = {}
    for spec in message.DESCRIPTOR.fields:
        value = getattr(message, spec.name)
        if spec.message_type is not None and spec.message_type.GetOptions().map_entry:
            value_spec = spec.message_type.fields_by_name["value"]
            tree[spec.name] = {str(k): _protobuf_value(value_spec, v) for k, v in sorted(value.items())}
        elif spec.is_repeated:
            tree[spec.name] = [_protobuf_value(spec, item) for item in value]
        elif spec.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
            tree[spec.name] = protobuf_tree(value) if message.HasField(spec.name) else None
        else:
            tree[spec.name] = _protobuf_value(spec, value)
    return tree


def _protobuf_value(spec: Any, value: Any) -> Any:
    if spec.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
        return protobuf_tree(value)
    if spec.cpp_type == FieldDescriptor.CPPTYPE_ENUM:
        named = spec.enum_type.values_by_number.get(value)
        return named.name if named is not None else value
    return value


def _chunk_messages(chunk: Chunk) -> list[Message]:
    if chunk.compression == "zstd":
        raw = zstandard.decompress(chunk.data, chunk.uncompressed_size)
    elif chunk.compression == "lz4":
        raw = lz4.frame.LZ4FrameDecompressor().decompress(chunk.data, max_length=chunk.uncompressed_size + 1)
    elif chunk.compression == "":
        raw = chunk.data
    else:
        raise McapError(f"unsupported compression {chunk.compression!r}")
    if len(raw) != chunk.uncompressed_size:
        raise McapError(f"chunk decompressed to {len(raw)} bytes, not the declared {chunk.uncompressed_size}")
    if chunk.uncompressed_crc and zlib.crc32(raw) != chunk.uncompressed_crc:
        raise McapError("chunk checksum does not match its decompressed data")
    records = breakup_chunk(
        Chunk(
            compression="",
            data=raw,
            message_end_time=chunk.message_end_time,
            message_start_time=chunk.message_start_time,
            uncompressed_crc=0,
            uncompressed_size=len(raw),
        )
    )
    return [record for record in records if isinstance(record, Message)]


def _read_summary(stream_reader: StreamReader) -> Summary:
    summary = Summary()
    for record in stream_reader.records:
        if isinstance(record, Statistics):
            summary.statistics = record
        elif isinstance(record, Schema):
            summary.schemas[record.id] = record
        elif isinstance(record, Channel):
            summary.channels[record.id] = record
        elif isinstance(record, AttachmentIndex):
            summary.attachment_indexes.append(record)
        elif isinstance(record, ChunkIndex):
            summary.chunk_indexes.append(record)
        elif isinstance(record, MetadataIndex):
            summary.metadata_indexes.append(record)
        elif isinstance(record, Footer):
            break
    return summary
