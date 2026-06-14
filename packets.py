DISCOVER = "10"
HELLO = "20"
TOKEN = "1000"
DATA = "2000"

def build_discover(nickname: str, ip: str) -> str:
    return f"{DISCOVER}:{nickname}:{ip}"

def build_hello(nickname: str, ip: str) -> str:
    return f"{HELLO}:{nickname}:{ip}"

def build_token() -> str:
    return f"{TOKEN}"

def build_data(origin, destination, control, crc, message) -> str:
    return f"{DATA}:{origin}:{destination}:{control}:{crc}:{message}"

def parse_packet(raw: str):
    parts = raw.split(':', 1)
    packet_type = parts[0]

    if packet_type == DATA:
        rest = parts[1].split(':', 4)
        return {
            'type': DATA,
            'origin': rest[0],
            'destination': rest[1],
            'control': rest[2],
            'crc': rest[3],
            'message': rest[4]
        }

    rest = parts[1].split(':', 2)

    if packet_type == DISCOVER:
        return {
            'type': DISCOVER,
            'nickname': rest[0],
            'ip': rest[1]
        }
    elif packet_type == HELLO:
        return {
            'type': HELLO,
            'nickname': rest[0],
            'ip': rest[1]
        }
    elif packet_type == TOKEN:
        return {
            'type': TOKEN
        }
    else:
        raise ValueError("Unknown packet type")