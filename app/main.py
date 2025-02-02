import socket


def _process_connection(conn: socket.SocketType):
    _request = conn.recv(512)
    length = int(42).to_bytes(length=4)

    correlation_id = 7
    header = correlation_id.to_bytes(length=4)

    response = length + header
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
