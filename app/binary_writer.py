from app.uuid import from_uuid
from app.varint import encode_varint


class BinaryWriter:
    def write_byte(self, number: int) -> bytes:
        return number.to_bytes(length=1)

    def write_int16(self, number: int) -> bytes:
        return number.to_bytes(length=2)

    def write_int32(self, number: int) -> bytes:
        return number.to_bytes(length=4)

    def write_int64(self, number: int) -> bytes:
        return number.to_bytes(length=8)

    def write_compact_string(self, string: str) -> bytes:
        return encode_varint(len(string) + 1) + string.encode()

    def write_compact_array(self, array: list[bytes]) -> bytes:
        # Compact arrays use N + 1 as a varint for their length
        return encode_varint(len(array) + 1) + b"".join(array)

    def write_records(self, records: bytes) -> bytes:
        return encode_varint(len(records) + 1) + records

    def write_uuid(self, uuid_str: str) -> bytes:
        return from_uuid(uuid_str)
