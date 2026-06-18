import zlib

def calculate_crc(message: str) -> int:
    return zlib.crc32(message.encode("utf-8")) & 0xffffffff

def is_valid_crc(message: str, expected_crc: int) -> bool:
    return calculate_crc(message) == expected_crc