NULL_BYTE = b"\x00"


def encode_varint(number):
    """
    Encode an integer as a varint.

    Args:
        number (int): The number to encode (must be non-negative)

    Returns:
        bytes: The encoded varint as bytes

    Raises:
        ValueError: If the number is negative
    """
    if number < 0:
        raise ValueError("Varint encoding only supports non-negative numbers")

    encoded = bytearray()

    while True:
        # Get the least significant 7 bits
        byte = number & 0x7F

        # Right shift the number by 7 bits
        number >>= 7

        # If there are more bits to encode, set the MSB to 1
        if number:
            byte |= 0x80

        encoded.append(byte)

        if not number:
            break

    return bytes(encoded)
