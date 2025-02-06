from abc import ABC
from itertools import zip_longest

from app.binary_reader import BinaryReader
from app.cluster_metadata import ClusterMetadata, PartitionRecord, TopicRecord
from app.constants import NULL_BYTE
from app.topic_data import TopicData
from app.uuid import from_uuid, to_uuid
from app.varint import encode_varint


class KafkaMessage(ABC):
    API_KEY: int
    MIN_VERSION: int
    MAX_VERSION: int

    def handle_request(self, request_body: bytes) -> bytes:
        raise NotImplementedError()


class ApiVersions(KafkaMessage):
    """
    https://kafka.apache.org/protocol.html#The_Messages_ApiVersions

    ApiVersions Request (Version: 4) => client_software_name client_software_version TAG_BUFFER
        client_software_name => COMPACT_STRING
        client_software_version => COMPACT_STRING

    ApiVersions Response (Version: 3/4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
    error_code => INT16
    api_keys => api_key min_version max_version TAG_BUFFER
        api_key => INT16
        min_version => INT16
        max_version => INT16
    throttle_time_ms => INT32
    """

    API_KEY = 18
    MIN_VERSION = 0
    MAX_VERSION = 4

    UNSUPPORTED_VERSION = 35
    SUPPORTED_API_VERSIONS = [0, 1, 2, 3, 4]

    def __init__(self, request_api_version: int):
        self.request_api_version = request_api_version

    def handle_request(self, request_body: bytes) -> bytes:
        with BinaryReader(raw_data=request_body) as reader:
            client_id_length = int.from_bytes(reader.read_bytes(2))
            _client_id = reader.read_bytes(client_id_length).decode()

        # Messages we support
        messages: list[type[KafkaMessage]] = [
            ApiVersions,
            DescribeTopicPartitions,
            Fetch,
        ]

        # Compact arrays use N + 1 for their length,
        # for ex., for a single key (N == 1) we need length 2
        num_api_keys = int(len(messages) + 1).to_bytes(length=1)

        # INT16
        error_code = (
            self.UNSUPPORTED_VERSION
            if self.request_api_version not in self.SUPPORTED_API_VERSIONS
            else 0
        ).to_bytes(length=2)

        # INT32
        throttle_time_ms = int(0).to_bytes(length=4)
        # An empty tagged field array, represented by a single byte of value 0x00
        tag_buffer = b"\x00"

        # Response body
        body = error_code + num_api_keys

        for message in messages:
            body += message.API_KEY.to_bytes(length=2)
            body += message.MIN_VERSION.to_bytes(length=2)
            body += message.MAX_VERSION.to_bytes(length=2)
            body += tag_buffer

        body += throttle_time_ms + tag_buffer

        return body


class DescribeTopicPartitions(KafkaMessage):
    """
    https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions

    DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER
        topics => name TAG_BUFFER
            name => COMPACT_STRING
        response_partition_limit => INT32
        cursor => topic_name partition_index TAG_BUFFER
            topic_name => COMPACT_STRING
            partition_index => INT32

    DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor TAG_BUFFER
        throttle_time_ms => INT32
        topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
            error_code => INT16
            name => COMPACT_NULLABLE_STRING
            topic_id => UUID
            is_internal => BOOLEAN
            partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas] TAG_BUFFER
                error_code => INT16
                partition_index => INT32
                leader_id => INT32
                leader_epoch => INT32
                replica_nodes => INT32
                isr_nodes => INT32
                eligible_leader_replicas => INT32
                last_known_elr => INT32
                offline_replicas => INT32
            topic_authorized_operations => INT32
        next_cursor => topic_name partition_index TAG_BUFFER
            topic_name => COMPACT_STRING
            partition_index => INT32
    """

    API_KEY = 75
    MIN_VERSION = 0
    MAX_VERSION = 0

    NULL_CURSOR = 0xFF

    TOPIC_AUTHORIZED_OPERATIONS = 0b0000_1101_1111_1000
    """
    This corresponds to the following operations:
    READ (bit index 3 from the right)
    WRITE (bit index 4 from the right)
    CREATE (bit index 5 from the right)
    DELETE (bit index 6 from the right)
    ALTER (bit index 7 from the right)
    DESCRIBE (bit index 8 from the right)
    DESCRIBE_CONFIGS (bit index 10 from the right)
    ALTER_CONFIGS (bit index 11 from the right)
    """

    class ErrorCodes:
        NO_ERROR = 0
        UNKNOWN_TOPIC = 3

    def handle_request(self, request_body: bytes) -> bytes:
        with BinaryReader(raw_data=request_body) as reader:
            # Parse request body
            client_id_length = int.from_bytes(reader.read_bytes(2))
            _client_id = reader.read_bytes(client_id_length).decode()

            tag_buffer = reader.read_bytes(1)
            assert tag_buffer == NULL_BYTE, f"Expected null byte, got {tag_buffer}"

            _array_length = reader.read_bytes(1)

            topics = []
            while (topic_name_length := reader.read_bytes(1)) != NULL_BYTE:
                topic_name_length = int.from_bytes(topic_name_length) - 1
                topic_name = reader.read_bytes(topic_name_length).decode()
                tag_buffer = reader.read_bytes(1)
                assert tag_buffer == NULL_BYTE, f"Expected null byte, got {tag_buffer}"
                topics.append(topic_name)

        # Build response body

        # Parse the logfile to discover topics
        # https://binspec.org/kafka-cluster-metadata
        cluster_metadata = ClusterMetadata.parse()
        topic_records = [
            record.value
            for batch in cluster_metadata
            for record in batch.records
            if isinstance(record.value, TopicRecord)
            and record.value.type == TopicRecord.TYPE
            and record.value.topic_name in topics
        ]
        topic_values = []
        for topic_record, topic_name in zip_longest(topic_records, topics):
            partition_records = [
                record.value
                for batch in cluster_metadata
                for record in batch.records
                if isinstance(record.value, PartitionRecord)
                and record.value.type == PartitionRecord.TYPE
                and topic_record
                and record.value.topic_uuid == topic_record.topic_uuid
            ]
            partitions = [
                (
                    int.to_bytes(DescribeTopicPartitions.ErrorCodes.NO_ERROR, length=2)
                    + int.to_bytes(partition.partition_id, length=4)
                    + int.to_bytes(partition.leader_id, length=4)
                    + int.to_bytes(partition.leader_epoch, length=4)
                    # I'm lazy, so the rest are just mocks of empty arrays
                    + int.to_bytes(1, length=1)  # Replica nodes
                    + int.to_bytes(1, length=1)  # In-sync replicas (ISR)
                    + int.to_bytes(1, length=1)  # Eligible leader replicas (ELR)
                    + int.to_bytes(1, length=1)  # Last known ELR
                    + int.to_bytes(1, length=1)  # Offline replicas
                    + NULL_BYTE
                )
                for partition in partition_records
            ]

            error_code = (
                DescribeTopicPartitions.ErrorCodes.UNKNOWN_TOPIC
                if not topic_record
                else DescribeTopicPartitions.ErrorCodes.NO_ERROR
            )

            # Topic ID
            default_topic_id = "00000000-0000-0000-0000-00000000000000000000-0000-0000-0000-000000000000"

            # False
            is_internal = NULL_BYTE

            # Varint
            partitions_array_length = encode_varint(len(partitions) + 1)

            topic_authorized_operations = (
                DescribeTopicPartitions.TOPIC_AUTHORIZED_OPERATIONS
            )

            topic_name = topic_record.topic_name if topic_record else topic_name
            topic_id = (
                from_uuid(topic_record.topic_uuid)
                if topic_record
                else int(default_topic_id.replace("-", "")).to_bytes(length=16)
            )
            topic_values.append(
                error_code.to_bytes(length=2)  # INT16
                + encode_varint(len(topic_name) + 1)
                + topic_name.encode()
                + topic_id
                + is_internal
                + partitions_array_length
                + b"".join(partitions)
                + topic_authorized_operations.to_bytes(length=4)  # 4-byte bitfield
                + NULL_BYTE
            )

        # INT32
        throttle_time_ms = 0

        return (
            throttle_time_ms.to_bytes(length=4)  # INT32
            + encode_varint(len(topic_values) + 1)
            + b"".join(topic_values)
            # Cursor used for pagination
            + DescribeTopicPartitions.NULL_CURSOR.to_bytes(length=1)
            + NULL_BYTE
        )


class Fetch(KafkaMessage):
    """
    https://kafka.apache.org/protocol.html#The_Messages_Fetch

    Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
        max_wait_ms => INT32
        min_bytes => INT32
        max_bytes => INT32
        isolation_level => INT8
        session_id => INT32
        session_epoch => INT32
        topics => topic_id [partitions] TAG_BUFFER
            topic_id => UUID
            partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER
            partition => INT32
            current_leader_epoch => INT32
            fetch_offset => INT64
            last_fetched_epoch => INT32
            log_start_offset => INT64
            partition_max_bytes => INT32
        forgotten_topics_data => topic_id [partitions] TAG_BUFFER
            topic_id => UUID
            partitions => INT32
        rack_id => COMPACT_STRING

    Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
        throttle_time_ms => INT32
        error_code => INT16
        session_id => INT32
        responses => topic_id [partitions] TAG_BUFFER
            topic_id => UUID
            partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER
            partition_index => INT32
            error_code => INT16
            high_watermark => INT64
            last_stable_offset => INT64
            log_start_offset => INT64
            aborted_transactions => producer_id first_offset TAG_BUFFER
                producer_id => INT64
                first_offset => INT64
            preferred_read_replica => INT32
            records => COMPACT_RECORDS
    """

    API_KEY = 1
    MIN_VERSION = 0
    MAX_VERSION = 16

    class ErrorCodes:
        NO_ERROR = 0
        UNKNOWN_TOPIC = 100

    def handle_request(self, request_body: bytes) -> bytes:
        with BinaryReader(raw_data=request_body) as reader:
            _max_wait_ms = int.from_bytes(reader.read_bytes(4))
            _min_bytes = int.from_bytes(reader.read_bytes(4))
            _max_bytes = int.from_bytes(reader.read_bytes(4))
            _isolation_level = int.from_bytes(reader.read_bytes(1))
            session_id = int.from_bytes(reader.read_bytes(4))
            _session_epoch = int.from_bytes(reader.read_bytes(4))
            num_of_topics = reader.read_varint()[0] - 1

            cluster_metadata = ClusterMetadata.parse()

            responses: list[bytes] = []
            for _ in range(num_of_topics):
                topic_id = to_uuid(reader.read_bytes(16))
                partitions_length = reader.read_varint()[0] - 1
                partitions_index = int.from_bytes(reader.read_bytes(4))

                topic_record = next(
                    (
                        record.value
                        for batch in cluster_metadata
                        for record in batch.records
                        if isinstance(record.value, TopicRecord)
                        and record.value.type == TopicRecord.TYPE
                        and record.value.topic_uuid == topic_id
                    ),
                    None,
                )
                # Recover the associated log file using the topic name and
                # the partition index from the request
                topic_file = (
                    f"/tmp/kraft-combined-logs/{topic_record.topic_name}-{partitions_index}/00000000000000000000.log"
                    if topic_record
                    else ""
                )
                # This will dump the whole file as is
                topic_data = TopicData.dump(topic_file)

                error_code = (
                    Fetch.ErrorCodes.NO_ERROR
                    if topic_record
                    else Fetch.ErrorCodes.UNKNOWN_TOPIC
                )
                responses.append(
                    from_uuid(topic_id)
                    + encode_varint(partitions_length + 1)
                    + partitions_index.to_bytes(length=4)
                    + int(error_code).to_bytes(length=2)  # error code UNKNOWN_TOPIC
                    + int(0).to_bytes(length=8)  # high watermark
                    + int(0).to_bytes(length=8)  # last stable offset
                    + int(0).to_bytes(length=8)  # log start offset
                    + int(0).to_bytes(length=1)  # num aborted transactions
                    + int(0).to_bytes(length=4)  # preferred read replica
                    + encode_varint(len(topic_data) + 1)
                    + topic_data
                    # Topic metadata already has a NULL BYTE marking its end
                    + NULL_BYTE  # End of Partitions array
                    + NULL_BYTE  # End of responses array
                )

        # INT32
        throttle_time_ms = 0
        # INT16
        error_code = 0

        return (
            throttle_time_ms.to_bytes(length=4)
            + error_code.to_bytes(length=2)
            + session_id.to_bytes(length=4)
            + encode_varint(len(responses) + 1)
            + b"".join(responses)
            + NULL_BYTE  # End of response body
        )
