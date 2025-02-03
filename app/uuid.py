from binascii import hexlify, unhexlify


def to_uuid(raw_bytes: bytes) -> str:
    hexified = hexlify(raw_bytes).zfill(32).decode()
    return f"{hexified[:8]}-{hexified[8:12]}-{hexified[12:16]}-{hexified[16:20]}-{hexified[20:]}"


def from_uuid(uuid_str: str) -> bytes:
    clean_uuid = uuid_str.replace("-", "")
    return unhexlify(clean_uuid)
