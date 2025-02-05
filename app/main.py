import asyncio
from itertools import zip_longest

from app.binary_reader import BinaryReader
from app.cluster_metadata import (
    ClusterMetadata,
    PartitionRecord,
    TopicRecord,
)
from app.messages import ApiVersions, DescribeTopicPartitions, Fetch, KafkaMessage
from app.utils import NULL_BYTE, encode_varint
from app.uuid import from_uuid, to_uuid


UNSUPPORTED_VERSION = 35
SUPPORTED_API_VERSIONS = [0, 1, 2, 3, 4]


def _handle_api_versions_request(
    request_body: bytes, request_api_version: int
) -> bytes:
    client_id_length = int.from_bytes(request_body[:2])
    _client_id = request_body[2 : 2 + client_id_length].decode()

    # Messages we support
    messages: list[type[KafkaMessage]] = [ApiVersions, DescribeTopicPartitions, Fetch]

    # Compact arrays use N + 1 for their length,
    # for ex., for a single key (N == 1) we need length 2
    num_api_keys = int(len(messages) + 1).to_bytes(length=1)

    # INT16
    error_code = (
        UNSUPPORTED_VERSION if request_api_version not in SUPPORTED_API_VERSIONS else 0
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


def _handle_describe_topic_partitions_request(request_body: bytes) -> bytes:
    # Parse request body
    client_id_length = int.from_bytes(request_body[:2])
    offset = 2 + client_id_length
    _client_id = request_body[2 : 2 + client_id_length].decode()

    _tag_buffer = request_body[offset]
    offset += 1

    _array_length = request_body[offset]
    offset += 1

    topics = []
    while (topic_name_length := request_body[offset]) != 0x00:
        topic_name = request_body[offset + 1 : offset + topic_name_length].decode()
        offset += topic_name_length + 1  # +1 for the TAG_BUFFER in between topic names
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
        default_topic_id = (
            "00000000-0000-0000-0000-00000000000000000000-0000-0000-0000-000000000000"
        )

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


def _handle_fetch_request(request_body: bytes) -> bytes:
    reader = BinaryReader(raw_data=request_body)

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
            + int(0).to_bytes(length=1)  # compact records length
            + NULL_BYTE  # end of Partitions array
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


def _handle_request(request: bytes):
    # Request Header v2
    # https://kafka.apache.org/protocol.html#protocol_messages
    # First 4 bytes are the message size
    message_size = int.from_bytes(request[:4])

    # Next 2 bytes are the request API key
    # A Kafka request specifies the API its calling by using the request_api_key header field.
    request_api_key = int.from_bytes(request[4:6])

    # Next 2 bytes are the request API version
    # Requests use the header field request_api_version to specify the API version being requested.
    request_api_version = int.from_bytes(request[6:8])

    # Finally, the next 4 bytes are the correlation_id
    # This field lets clients match responses to their original requests
    correlation_id = int.from_bytes(request[8:12])

    request_body = request[12 : 12 + message_size]

    # Response Header v0
    # https://kafka.apache.org/protocol.html#protocol_messages
    # INT32
    header = correlation_id.to_bytes(length=4)
    if request_api_key == ApiVersions.API_KEY:
        body = _handle_api_versions_request(request_body, request_api_version)
    elif request_api_key == DescribeTopicPartitions.API_KEY:
        # Response Header v1 includes a tag buffer
        header += NULL_BYTE
        body = _handle_describe_topic_partitions_request(request_body)
    elif request_api_key == Fetch.API_KEY:
        # Response Header v1 includes a tag buffer
        header += NULL_BYTE

        # Request Header v2 includes the client_id before the request body
        client_id_length = int.from_bytes(request[12:14])
        offset = 14
        _client_id = request[offset : offset + client_id_length].decode()
        assert (
            request[offset + client_id_length + 1] == 0
        ), f"Expected null byte, got {request[offset + client_id_length + 1]}"

        offset += client_id_length + 1
        request_body = request[offset : offset + message_size]

        body = _handle_fetch_request(request_body)
    else:
        raise Exception(f"Unsupported API key: {request_api_key}")

    length = (len(header) + len(body)).to_bytes(length=4)
    response = length + header + body
    return response


async def _process_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    try:
        while request := await reader.read(512):
            response = _handle_request(request)
            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"Error processing connection: {e}")
        raise e
    finally:
        writer.close()
        await writer.wait_closed()


async def _run_server():
    server = await asyncio.start_server(
        _process_connection, host="localhost", port=9092
    )

    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        print("Shutting down server")
    finally:
        server.close()
        await server.wait_closed()


def main():
    try:
        asyncio.run(_run_server())
    except KeyboardInterrupt:
        print("Shutting down broker...")


if __name__ == "__main__":
    main()
