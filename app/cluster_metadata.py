from dataclasses import dataclass, field, fields

from app.uuid import to_uuid
from app.binary_reader import BinaryReader


@dataclass
class Metadata:
    # For debugging purposes
    def __setattr__(self, name, value):
        # print(f"Setting {value} to {name}")
        super().__setattr__(name, value)

    def __repr__(self):
        own_fields = ", ".join(
            [
                f"{field.name}={getattr(self, field.name, None)}"
                for field in fields(self)
            ]
        )
        return f"{self.__class__.__name__}({own_fields})"


@dataclass
class RecordValue(Metadata):
    frame_version: int = field(init=False)
    type: int = field(init=False)
    version: int = field(init=False)
    tagged_fields_count: int = field(init=False)


@dataclass
class PartitionRecord(RecordValue):
    TYPE = 0x03

    partition_id: int = field(init=False)
    topic_uuid: str = field(init=False)
    leader_id: int = field(init=False)
    leader_epoch: int = field(init=False)


@dataclass
class FeatureLevelRecord(RecordValue):
    TYPE = 0x0C

    name_length: int = field(init=False)
    name: int | None = field(init=False, default=None)


@dataclass
class TopicRecord(RecordValue):
    TYPE = 0x02

    name_length: int = field(init=False)
    topic_name: str = field(init=False)
    topic_uuid: str = field(init=False)


@dataclass
class Record(Metadata):
    length: int = field(init=False)
    attributes: int = field(init=False)
    timestamp_delta: int = field(init=False)
    offset_delta: int = field(init=False)
    key_length: int = field(init=False)
    key: int | None = field(init=False)
    value_length: int = field(init=False)
    value: TopicRecord | FeatureLevelRecord | PartitionRecord = field(init=False)
    headers_array_count: int = field(init=False)


@dataclass
class RecordBatch(Metadata):
    base_offset: int = field(init=False)
    batch_length: int = field(init=False)
    partition_leader_epoch: int = field(init=False)
    magic_byte: int = field(init=False)
    crc: int = field(init=False)
    attributes: int = field(init=False)
    last_offset_delta: int = field(init=False)
    base_timestamp: int = field(init=False)
    max_timestamp: int = field(init=False)
    producer_id: int = field(init=False)
    producer_epoch: int = field(init=False)
    base_sequence: int = field(init=False)
    records_length: int = field(init=False)
    records: list[Record] = field(init=False, default_factory=lambda: [])


class ClusterMetadata:
    # This follows the visualization of the hex file and associated byte-lengths in:
    # https://binspec.org/kafka-cluster-metadata
    FILE_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

    @classmethod
    def parse(cls) -> list[RecordBatch]:
        with BinaryReader(cls.FILE_PATH) as reader:
            batches: list[RecordBatch] = []
            while True:
                reader.rewind()

                # 12 to skip the base offset and batch length + total length of batches
                offset = (12 * len(batches)) + sum(
                    [batch.batch_length for batch in batches]
                )
                reader.skip(offset)

                if reader.peek_bytes(1) == b"":
                    # EOF
                    break

                record_batch = RecordBatch()
                record_batch.base_offset = int.from_bytes(reader.read_bytes(8))
                record_batch.batch_length = int.from_bytes(reader.read_bytes(4))
                record_batch.partition_leader_epoch = int.from_bytes(
                    reader.read_bytes(4)
                )
                record_batch.magic_byte = int.from_bytes(reader.read_bytes(1))
                record_batch.crc = int.from_bytes(reader.read_bytes(4))
                record_batch.attributes = int.from_bytes(reader.read_bytes(2))
                record_batch.last_offset_delta = int.from_bytes(reader.read_bytes(4))
                record_batch.base_timestamp = int.from_bytes(reader.read_bytes(8))
                record_batch.max_timestamp = int.from_bytes(reader.read_bytes(8))
                record_batch.producer_id = int.from_bytes(reader.read_bytes(8))
                record_batch.producer_epoch = int.from_bytes(reader.read_bytes(2))
                record_batch.base_sequence = int.from_bytes(reader.read_bytes(4))
                record_batch.records_length = int.from_bytes(reader.read_bytes(4))

                for _ in range(record_batch.records_length):
                    record = Record()
                    record.length = reader.read_varint(signed=True)[0]
                    record.attributes = int.from_bytes(reader.read_bytes(1))
                    record.timestamp_delta = int.from_bytes(reader.read_bytes(1))
                    record.offset_delta = int.from_bytes(reader.read_bytes(1))
                    record.key_length = reader.read_varint(signed=True)[0]
                    record.key = None
                    if record.key_length == -1:
                        # No key to parse
                        pass
                    else:
                        record.key = reader.read_bytes(record.key_length)
                    record.value_length = reader.read_varint(signed=True)[0]

                    frame_version = int.from_bytes(reader.read_bytes(1))
                    record_type = int.from_bytes(reader.read_bytes(1))
                    value: FeatureLevelRecord | TopicRecord | PartitionRecord
                    if record_type == FeatureLevelRecord.TYPE:
                        value = FeatureLevelRecord()
                        value.frame_version = frame_version
                        value.type = record_type
                        value.version = int.from_bytes(reader.read_bytes(1))
                        value.name_length = reader.read_varint()[0] - 1
                        if not value.name_length == -1:
                            value.name = reader.read_bytes(value.name_length).decode()
                        else:
                            value.name = None
                        value.feature_level = int.from_bytes(reader.read_bytes(2))
                        value.tagged_fields_count = reader.read_varint()[0]
                    elif record_type == TopicRecord.TYPE:
                        value = TopicRecord()
                        value.frame_version = frame_version
                        value.type = record_type
                        value.version = int.from_bytes(reader.read_bytes(1))
                        value.name_length = reader.read_varint()[0] - 1
                        value.topic_name = reader.read_bytes(value.name_length).decode()
                        value.topic_uuid = to_uuid(reader.read_bytes(16))
                        value.tagged_fields_count = reader.read_varint()[0]
                    elif record_type == PartitionRecord.TYPE:
                        value = PartitionRecord()
                        value.frame_version = frame_version
                        value.type = record_type
                        value.version = int.from_bytes(reader.read_bytes(1))
                        # Partition record
                        # partition_index leader_id leader_epoch
                        value.partition_id = int.from_bytes(reader.read_bytes(4))
                        value.topic_uuid = to_uuid(reader.read_bytes(16))
                        # Leap of faith - skip to leader info
                        # There are varints in here, so this is a best guess
                        reader.skip(1 + 4 + 1 + 4 + 1 + 1)
                        value.leader_id = int.from_bytes(reader.read_bytes(4))
                        value.leader_epoch = int.from_bytes(reader.read_bytes(4))
                        # Skip the rest, we got what we came for...
                        reader.skip(4 + 1 + 16)
                        # ...except for the tagged fields count, we gotta
                        # read that as it's a varint
                        value.tagged_fields_count = reader.read_varint()[0]
                    else:
                        raise Exception(f"Unknown record type: {record_type}")

                    record.value = value
                    record.headers_array_count = reader.read_varint()[0]

                    record_batch.records.append(record)

                batches.append(record_batch)
        return batches
