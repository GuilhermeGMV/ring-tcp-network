import socket

from packets import PACKET_LABELS, parse_packet
import ui


def listen(node):
    while node.running:
        try:
            data, address = node.socket.recvfrom(65535)
            packet = parse_packet(data.decode())
            label = PACKET_LABELS.get(packet["type"], packet["type"])
            ui.log(
                node.nickname,
                f"UDP recebido {label} <- {address[0]}:{address[1]} "
                f"({len(data)} bytes)",
            )
            node._handle_packet(packet, address[0])
        except socket.timeout:
            continue
        except OSError:
            break
        except Exception as error:
            ui.log(node.nickname, f"pacote ignorado: {error}")
