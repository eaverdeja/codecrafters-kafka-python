import asyncio

from app.kafka import ApiVersions, DescribeTopicPartitions, KafkaMessage


UNSUPPORTED_VERSION = 35
SUPPORTED_API_VERSIONS = [0, 1, 2, 3, 4]


def _handle_request(request: bytes):
    # Request Header v2
    # https://kafka.apache.org/protocol.html#protocol_messages
    # First 4 bytes are the message size
    message_size = int.from_bytes(request[:4])

    # Next 2 bytes are the request API key
    # A Kafka request specifies the API its calling by using the request_api_key header field.
    _request_api_key = int.from_bytes(request[4:6])

    # Next 2 bytes are the request API version
    # Requests use the header field request_api_version to specify the API version being requested.
    request_api_version = int.from_bytes(request[6:8])

    # Finally, the next 4 bytes are the correlation_id
    # This field lets clients match responses to their original requests
    correlation_id = int.from_bytes(request[8:12])

    request_body = request[12 : 12 + message_size]

    client_id_length = int.from_bytes(request_body[:2])
    _client_id = request_body[2 : 2 + client_id_length].decode()

    # Response Header v0
    # https://kafka.apache.org/protocol.html#protocol_messages
    # INT32
    header = correlation_id.to_bytes(length=4)

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
    # Null byte
    tag_buffer = b"\x00"

    # Response body
    body = error_code + num_api_keys

    for message in messages:
        body += message.API_KEY.to_bytes(length=2)
        body += message.MIN_VERSION.to_bytes(length=2)
        body += message.MAX_VERSION.to_bytes(length=2)
        body += tag_buffer

    body += throttle_time_ms + tag_buffer

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
