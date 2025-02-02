import socket


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    server.accept()


if __name__ == "__main__":
    main()
