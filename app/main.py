import asyncio
import traceback


from app.binary_reader import BinaryReader
from app.binary_writer import BinaryWriter
from app.constants import NULL_BYTE
from app.messages import ApiVersions, DescribeTopicPartitions, Fetch


def _handle_request(request: bytes):
    with BinaryReader(raw_data=request) as reader:
        # Request Header v2
        # https://kafka.apache.org/protocol.html#protocol_messages
        # First 4 bytes are the message size
        message_size = int.from_bytes(reader.read_bytes(4))

        # Next 2 bytes are the request API key
        # A Kafka request specifies the API its calling by using the request_api_key header field.
        request_api_key = int.from_bytes(reader.read_bytes(2))

        # Next 2 bytes are the request API version
        # Requests use the header field request_api_version to specify the API version being requested.
        request_api_version = int.from_bytes(reader.read_bytes(2))

        # Finally, the next 4 bytes are the correlation_id
        # This field lets clients match responses to their original requests
        correlation_id = int.from_bytes(reader.read_bytes(4))

        request_body = reader.read_bytes(message_size)

    # Response Header v0
    # https://kafka.apache.org/protocol.html#protocol_messages
    writer = BinaryWriter()
    header = writer.write_int32(correlation_id)

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
            with BinaryReader(raw_data=request_body) as reader:
                client_id_length = int.from_bytes(reader.read_bytes(2))
                _client_id = reader.read_bytes(client_id_length).decode()
                tag_buffer = reader.read_bytes(1)
                assert tag_buffer == NULL_BYTE, f"Expected null byte, got {tag_buffer}"
                request_body = reader.read_bytes(message_size)

            message = Fetch()
        case _:
            raise Exception(f"Unsupported API key: {request_api_key}")

    body = message.handle_request(request_body)

    length = writer.write_int32(len(header) + len(body))
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
