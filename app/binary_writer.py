from app.varint import encode_varint


class BinaryWriter:
    def write_int16(self, number: int) -> bytes:
        return number.to_bytes(length=2)

    def write_int32(self, number: int) -> bytes:
        return number.to_bytes(length=4)

    def write_compact_array(self, array: list[bytes]) -> bytes:
        # Compact arrays use N + 1 as a varint for their length
        return encode_varint(len(array) + 1) + b"".join(array)
