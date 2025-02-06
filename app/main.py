import asyncio
import traceback


from app.constants import NULL_BYTE
from app.messages import ApiVersions, DescribeTopicPartitions, Fetch


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
    message: ApiVersions | DescribeTopicPartitions | Fetch
    match request_api_key:
        case ApiVersions.API_KEY:
            message = ApiVersions(request_api_version)
        case DescribeTopicPartitions.API_KEY:
            # Response Header v1 includes a tag buffer
            header += NULL_BYTE

            message = DescribeTopicPartitions()
        case Fetch.API_KEY:
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

            message = Fetch()
        case _:
            raise Exception(f"Unsupported API key: {request_api_key}")

    body = message.handle_request(request_body)

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
        print(traceback.format_exc())
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
