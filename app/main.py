import asyncio

from app.kafka import ApiVersions, DescribeTopicPartitions, KafkaMessage
from app.utils import NULL_BYTE, encode_varint


UNSUPPORTED_VERSION = 35
SUPPORTED_API_VERSIONS = [0, 1, 2, 3, 4]


def _handle_api_versions_request(
    request_body: bytes, request_api_version: int
) -> bytes:
    client_id_length = int.from_bytes(request_body[:2])
    _client_id = request_body[2 : 2 + client_id_length].decode()

    # Messages we support
    messages: list[type[KafkaMessage]] = [
        ApiVersions,
        DescribeTopicPartitions,
    ]

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

    topic_name_length = request_body[offset]
    topic_name = request_body[offset + 1 : offset + topic_name_length].decode()

    # Build response body
    error_code = DescribeTopicPartitions.ErrorCodes.UNKNOWN_TOPIC
    # INT32
    throttle_time_ms = 0

    # UUID
    topic_id = (
        "00000000-0000-0000-0000-00000000000000000000-0000-0000-0000-000000000000"
    )

    # False
    is_internal = NULL_BYTE

    # Varint - empty array
    partitions_array_length = encode_varint(1)

    topic_authorized_operations = DescribeTopicPartitions.TOPIC_AUTHORIZED_OPERATIONS

    return (
        throttle_time_ms.to_bytes(length=4)  # INT32
        + encode_varint(2)  # varint topic length
        + error_code.to_bytes(length=2)  # INT16
        + encode_varint(len(topic_name) + 1)
        + topic_name.encode()
        + int(topic_id.replace("-", "")).to_bytes(length=16)  # UUID
        + is_internal
        + partitions_array_length
        + topic_authorized_operations.to_bytes(length=4)  # 4-byte bitfield
        + NULL_BYTE
        + DescribeTopicPartitions.NULL_CURSOR.to_bytes(length=1)
        + NULL_BYTE
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
    if request_api_key == 18:
        body = _handle_api_versions_request(request_body, request_api_version)
    elif request_api_key == 75:
        header += b"\x00"
        body = _handle_describe_topic_partitions_request(request_body)
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
