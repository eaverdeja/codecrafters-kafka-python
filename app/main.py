import socket


def _process_connection(conn: socket.SocketType):
    request = conn.recv(512)

    # Request Header v2
    # https://kafka.apache.org/protocol.html#protocol_messages
    # First 4 bytes are the message size
    _message_size = int.from_bytes(request[:4])

    # Next 2 bytes are the request API key
    # A Kafka request specifies the API its calling by using the request_api_key header field.
    _request_api_key = int.from_bytes(request[4:6])

    # Next 2 bytes are the request API version
    # Requests use the header field request_api_version to specify the API version being requested.
    _request_api_version = int.from_bytes(request[6:8])

    # Finally, the next 4 bytes are the correlation_id
    # This field lets clients match responses to their original requests
    correlation_id = int.from_bytes(request[8:12])

    # Keep the length hardcoded for now
    length = int(42).to_bytes(length=4)
    # Response Header v0
    header = correlation_id.to_bytes(length=4)

    error_code = int(35).to_bytes(length=2)
    body = error_code

    response = length + header + body
    conn.send(response)
    conn.close()


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    try:
        while True:
            conn, _ = server.accept()
            _process_connection(conn)
    except KeyboardInterrupt:
        print("Shutting down broker...")
    finally:
        server.close()


if __name__ == "__main__":
    main()
