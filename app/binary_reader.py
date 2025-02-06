from io import BufferedReader, BytesIO


class BinaryReader:
    stream: BufferedReader | BytesIO

    def __init__(self, filename: str | None = None, raw_data: bytes | None = None):
        if filename:
            self.stream = open(filename, "rb")
        elif raw_data:
            self.stream = BytesIO(raw_data)
        else:
            raise Exception("Expected filename or raw data")

        self.position = 0

    def seek(self, offset, whence=0):
        """
        Move cursor to a new position
        whence: 0 (start), 1 (current), 2 (end)
        Returns new absolute position
        """
        self.position = self.stream.seek(offset, whence)
        return self.position

    def tell(self):
        """Return current cursor position"""
        return self.stream.tell()

    def read_bytes(self, size):
        """Read specified number of bytes from current position"""
        data = self.stream.read(size)
        self.position += len(data)
        return data

    def peek_bytes(self, size):
        """Read bytes without moving cursor"""
        current_pos = self.tell()
        data = self.read_bytes(size)
        self.seek(current_pos)
        return data

    def read_all(self):
        return self.stream.read()

    def skip(self, size):
        """Skip ahead by size bytes"""
        return self.seek(size, 1)

    def read_varint(self, signed: bool = False):
        """
        Read a variable-length integer from the current position.
        Returns tuple of (value, bytes_read)
        """
        result = 0
        shift = 0
        bytes_read = 0

        while True:
            byte = ord(self.read_bytes(1))
            bytes_read += 1

            # Add the lower 7 bits to the result
            result |= (byte & 0x7F) << shift
            shift += 7

            # If the high bit is not set, we're done
            if not (byte & 0x80):
                break

            # Protect against malformed data
            if shift >= 64:
                raise ValueError("Varint is too long")

        if signed:
            # ZigZag decode: Convert from ZigZag encoding back to signed
            result = (result >> 1) ^ (-(result & 1))

        return result, bytes_read

    def rewind(self):
        """Return to start of file"""
        return self.seek(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stream.close()
