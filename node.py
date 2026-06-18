from dataclasses import dataclass
from queue import Queue
import socket
import threading
import time

from crc import calculate_crc, is_valid_crc
from fault import maybe_corrupt
from packets import (
    ACK,
    BROADCAST,
    DATA,
    DISCOVER,
    HELLO,
    MACHINE_NOT_FOUND,
    NAK,
    TOKEN,
    build_data,
    build_discover,
    build_hello,
    build_token,
    parse_packet,
)
from topology import Topology
import ui


PORT = 6000
DISCOVERY_TIME = 2


@dataclass
class Node:
    nickname: str
    token_data_time: float
    error_probability: float
    token_timeout: float
    min_token_interval: float

    def start(self):
        self.ip = self._get_local_ip()
        self.topology = Topology({self.nickname: self.ip})
        self.queue = Queue(maxsize=10)
        self.socket = self._create_socket()
        self.running = True
        self.waiting_data = False
        self.remove_token = False
        self.controls_token = False
        self.last_token_time = None

        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.monitor_token, daemon=True).start()

        ui.log(self.nickname, f"iniciado em {self.ip}:{PORT}")
        self._broadcast(build_discover(self.nickname, self.ip))
        ui.log(self.nickname, "DISCOVER enviado")

        time.sleep(DISCOVERY_TIME)
        self._create_first_token_if_needed()

        ui.show_help()
        self._command_loop()

    def _create_socket(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_socket.settimeout(0.5)
        udp_socket.bind(("", PORT))
        return udp_socket

    def _get_local_ip(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            udp_socket.connect(("8.8.8.8", 80))
            return udp_socket.getsockname()[0]
        except OSError:
            try:
                return socket.gethostbyname(socket.gethostname())
            except OSError:
                return "127.0.0.1"
        finally:
            udp_socket.close()

    def listen(self):
        while self.running:
            try:
                data, _ = self.socket.recvfrom(65535)
                packet = parse_packet(data.decode())
                self._handle_packet(packet)
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception as error:
                ui.log(self.nickname, f"pacote ignorado: {error}")


    def _handle_packet(self, packet):
        
        if packet["type"] == DISCOVER:
            self._handle_discover(packet)

        elif packet["type"] == HELLO:
            self._add_machine(packet["nickname"], packet["ip"])

        elif packet["type"] == TOKEN:
            self._handle_token()

        elif packet["type"] == DATA:
            self._handle_data(packet)

    
    def _handle_discover(self, packet):
        # pula se tiver o mesmo nickname
        if packet["nickname"] == self.nickname:
            return

        # salva na topologia
        self._add_machine(packet["nickname"], packet["ip"])
        # responde com HELLO
        self._broadcast(build_hello(self.nickname, self.ip))
        ui.log(self.nickname, "HELLO enviado")


    def _add_machine(self, nickname, ip):
        # pula se tiver o mesmo nickname
        if nickname == self.nickname:
            return

        # se não tiver na topologia ou tiver com IP diferente, adiciona/atualiza
        if self.topology.machines.get(nickname) != ip:
            self.topology.machines[nickname] = ip
            ui.log(self.nickname, f"topologia: {self._ring_text()}")


    def _create_first_token_if_needed(self):
        first = sorted(self.topology.machines)[0]
        if first == self.nickname:
            self.controls_token = True
            self.last_token_time = time.time()
            ui.log(self.nickname, "gerando TOKEN inicial")
            self._send_to_successor(build_token(), "TOKEN")


    def _handle_token(self):
        ui.log(self.nickname, "TOKEN recebido")

        if self.remove_token:
            self.remove_token = False
            ui.log(self.nickname, "TOKEN retirado do anel")
            return

        if self.controls_token:
            now = time.time()
            if self.last_token_time is not None:
                elapsed = now - self.last_token_time
                if elapsed < self.min_token_interval:
                    ui.log(self.nickname, "mais de um TOKEN detectado; retirando este")
                    return
            self.last_token_time = now

        if self.waiting_data:
            self._send_to_successor(build_token(), "TOKEN")
            return

        if self.queue.empty():
            self._send_to_successor(build_token(), "TOKEN")
            return

        self._send_queued_message()


    def _send_queued_message(self):
        item = self.queue.queue[0]
        message = item["message"]
        crc = calculate_crc(message)

        if item["destination"] != BROADCAST and item["tries"] == 0:
            message = maybe_corrupt(message, self.error_probability)

        item["tries"] += 1
        packet = build_data(
            self.nickname,
            item["destination"],
            MACHINE_NOT_FOUND,
            crc,
            message,
        )
        self.waiting_data = True
        ui.log(self.nickname, f"DADOS enviados para {item['destination']}")
        self._send_to_successor(packet, "DADOS")


    def _handle_data(self, packet):
        ui.log(
            self.nickname,
            f"DADOS {packet['origin']} -> {packet['destination']} [{packet['control']}]",
        )

        if packet["origin"] == self.nickname:
            self._handle_returned_data(packet)

        elif packet["destination"] == self.nickname:
            self._handle_my_data(packet)

        elif packet["destination"] == BROADCAST:
            ui.log(self.nickname, f"broadcast de {packet['origin']}: {packet['message']}")
            self._forward_data(packet)
            
        else:
            self._forward_data(packet)


    def _handle_my_data(self, packet):
        ui.log(self.nickname, f"mensagem de {packet['origin']}: {packet['message']}")

        if is_valid_crc(packet["message"], int(packet["crc"])):
            packet["control"] = ACK
            ui.log(self.nickname, "CRC valido; ACK")
        else:
            packet["control"] = NAK
            ui.log(self.nickname, "CRC invalido; NAK")

        self._forward_data(packet)


    def _handle_returned_data(self, packet):
        if self.queue.empty():
            self._release_token()
            return

        item = self.queue.queue[0]

        if packet["control"] == ACK:
            ui.log(self.nickname, "ACK recebido")
            self.queue.get()
        elif packet["control"] == NAK and item["tries"] < 2:
            ui.log(self.nickname, "NAK recebido; retransmitindo na proxima passagem")
        elif packet["control"] == NAK:
            ui.log(self.nickname, "NAK recebido novamente; descartando mensagem")
            self.queue.get()
        elif item["destination"] == BROADCAST:
            ui.log(self.nickname, "broadcast completou uma volta")
            self.queue.get()
        else:
            ui.log(self.nickname, f"maquina {item['destination']} inexistente")
            self.queue.get()

        self._release_token()


    def _forward_data(self, packet):
        raw = build_data(
            packet["origin"],
            packet["destination"],
            packet["control"],
            packet["crc"],
            packet["message"],
        )
        self._send_to_successor(raw, "DADOS")


    def _release_token(self):
        self.waiting_data = False
        self._send_to_successor(build_token(), "TOKEN")


    def _send_to_successor(self, packet, label):
        time.sleep(self.token_data_time)
        successor, ip = self.topology.get_successor(self.nickname)
        self.socket.sendto(packet.encode(), (ip, PORT))
        ui.log(self.nickname, f"{label} enviado para {successor}")


    def _broadcast(self, packet):
        self.socket.sendto(packet.encode(), ("255.255.255.255", PORT))


    def monitor_token(self):
        while self.running:
            time.sleep(0.5)
            if not self.controls_token or self.last_token_time is None:
                continue

            if time.time() - self.last_token_time > self.token_timeout:
                ui.log(self.nickname, "TOKEN perdido; gerando novo")
                self.last_token_time = time.time()
                self._send_to_successor(build_token(), "TOKEN")


    def _command_loop(self):
        while self.running:
            try:
                command = ui.read_command()
            except (EOFError, KeyboardInterrupt):
                break

            if command == "sair":
                break
            elif command == "ajuda":
                ui.show_help()
            elif command == "topologia":
                ui.show_topology(self.topology.machines)
            elif command == "fila":
                ui.show_queue(list(self.queue.queue))
            elif command == "token adicionar":
                self._send_to_successor(build_token(), "TOKEN")
            elif command == "token remover":
                self.remove_token = True
                ui.log(self.nickname, "o proximo TOKEN sera removido")
            elif command.startswith("mensagem "):
                self._add_message(command)
            else:
                ui.log(self.nickname, "comando desconhecido")

        self.running = False
        self.socket.close()


    def _add_message(self, command):
        parts = command.split(" ", 2)
        if len(parts) < 3:
            ui.log(self.nickname, "uso: mensagem <destino|BROADCAST> <texto>")
            return

        if self.queue.full():
            ui.log(self.nickname, "fila cheia")
            return

        self.queue.put({
            "destination": parts[1],
            "message": parts[2],
            "tries": 0,
        })
        ui.log(self.nickname, f"mensagem para {parts[1]} adicionada")


    def _ring_text(self):
        ordered = sorted(self.topology.machines)
        return " -> ".join(ordered + ordered[:1])


def create_node(path: str) -> Node:
    with open(path, 'r') as f:
        lines = f.read().splitlines()
        return Node(
            nickname=lines[0].strip(),
            token_data_time=float(lines[1].strip().replace(',', '.')),
            error_probability=float(lines[2].strip().replace(',', '.')),
            token_timeout=float(lines[3].strip().replace(',', '.')),
            min_token_interval=float(lines[4].strip().replace(',', '.'))
        )
