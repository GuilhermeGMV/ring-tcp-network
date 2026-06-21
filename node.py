from dataclasses import dataclass
import ipaddress
from queue import Queue
import socket
import struct
import threading
import time

try:
    import fcntl
except ImportError:
    fcntl = None

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
DISCOVERY_ATTEMPTS = 3
DISCOVERY_INTERVAL = 1
SIOCGIFADDR = 0x8915
SIOCGIFNETMASK = 0x891B
PACKET_LABELS = {
    DISCOVER: "DISCOVER",
    HELLO: "HELLO",
    TOKEN: "TOKEN",
    DATA: "DADOS",
}


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

        log_path = ui.start_log(self.nickname)
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.monitor_token, daemon=True).start()

        ui.log(self.nickname, f"iniciado em {self.ip}:{PORT}")
        ui.log(self.nickname, f"broadcast local: {self._broadcast_addresses()[0]}")
        for attempt in range(DISCOVERY_ATTEMPTS):
            self._broadcast(build_discover(self.nickname, self.ip))
            ui.log(self.nickname, f"DISCOVER enviado ({attempt + 1}/{DISCOVERY_ATTEMPTS})")
            time.sleep(DISCOVERY_INTERVAL)

        self._create_first_token_if_needed()

        ui.show_message(f"Logs de rede: {log_path}")
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
        for target in (("8.8.8.8", 80), ("1.1.1.1", 80), ("255.255.255.255", PORT)):
            udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                if target[0] == "255.255.255.255":
                    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                udp_socket.connect(target)
                ip = udp_socket.getsockname()[0]
                if ip and not ip.startswith("127."):
                    return ip
            except OSError:
                pass
            finally:
                udp_socket.close()

        try:
            for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
                if not ip.startswith("127."):
                    return ip
        except OSError:
            pass

        return "127.0.0.1"

    def listen(self):
        while self.running:
            try:
                data, address = self.socket.recvfrom(65535)
                packet = parse_packet(data.decode())
                label = PACKET_LABELS.get(packet["type"], packet["type"])
                ui.log(
                    self.nickname,
                    f"UDP recebido {label} <- {address[0]}:{address[1]} "
                    f"({len(data)} bytes)",
                )
                self._handle_packet(packet, address[0])
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception as error:
                ui.log(self.nickname, f"pacote ignorado: {error}")


    def _handle_packet(self, packet, source_ip):
        if packet["type"] == DISCOVER:
            self._handle_discover(packet, source_ip)

        elif packet["type"] == HELLO:
            self._add_machine(packet["nickname"], self._packet_ip(packet, source_ip))

        elif packet["type"] == TOKEN:
            self._handle_token()

        elif packet["type"] == DATA:
            self._handle_data(packet)

    def _handle_discover(self, packet, source_ip):
        # pula se tiver o mesmo nickname
        if packet["nickname"] == self.nickname:
            return

        # salva na topologia
        remote_ip = self._packet_ip(packet, source_ip)
        self._add_machine(packet["nickname"], remote_ip)
        # responde com HELLO direto para o remetente e tambem por broadcast
        self._send_direct(build_hello(self.nickname, self.ip), remote_ip)
        self._broadcast(build_hello(self.nickname, self.ip))
        ui.log(self.nickname, f"HELLO enviado para {packet['nickname']}")


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
        data_message = (
            f"DADOS {packet['origin']} -> {packet['destination']} "
            f"[control={packet['control']}]"
        )
        ui.log(self.nickname, data_message)

        if packet["destination"] == self.nickname or packet["origin"] == self.nickname:
            ui.show_message(f"mensagem de {packet['origin']}: {data_message}")

        if packet["origin"] == self.nickname:
            self._handle_returned_data(packet)

        elif packet["destination"] == self.nickname:
            self._handle_my_data(packet)

        elif packet["destination"] == BROADCAST:
            ui.log(self.nickname, f"broadcast de {packet['origin']}: {packet['message']}")
            ui.show_message(f"broadcast de {packet['origin']}: {packet['message']}")
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
        ui.log(
            self.nickname,
            f"UDP enviando {label} -> {successor} ({ip}:{PORT})",
        )
        try:
            sent_bytes = self.socket.sendto(packet.encode(), (ip, PORT))
            ui.log(
                self.nickname,
                f"UDP enviado {label} -> {successor} ({ip}:{PORT}) "
                f"({sent_bytes} bytes)",
            )
        except OSError as error:
            ui.log(
                self.nickname,
                f"UDP falhou {label} -> {successor} ({ip}:{PORT}): {error}",
            )
            raise


    def _broadcast(self, packet):
        packet_type = packet.split(":", 1)[0]
        label = PACKET_LABELS.get(packet_type, packet_type)
        for address in self._broadcast_addresses():
            ui.log(
                self.nickname,
                f"UDP enviando {label} por broadcast -> {address}:{PORT}",
            )
            try:
                sent_bytes = self.socket.sendto(packet.encode(), (address, PORT))
                ui.log(
                    self.nickname,
                    f"UDP enviado {label} por broadcast -> {address}:{PORT} "
                    f"({sent_bytes} bytes)",
                )
            except OSError as error:
                ui.log(
                    self.nickname,
                    f"UDP falhou {label} por broadcast -> {address}:{PORT}: {error}",
                )


    def _send_direct(self, packet, ip):
        packet_type = packet.split(":", 1)[0]
        label = PACKET_LABELS.get(packet_type, packet_type)
        ui.log(self.nickname, f"UDP enviando {label} por unicast -> {ip}:{PORT}")
        try:
            sent_bytes = self.socket.sendto(packet.encode(), (ip, PORT))
            ui.log(
                self.nickname,
                f"UDP enviado {label} por unicast -> {ip}:{PORT} "
                f"({sent_bytes} bytes)",
            )
        except OSError as error:
            ui.log(
                self.nickname,
                f"UDP falhou {label} por unicast -> {ip}:{PORT}: {error}",
            )
            raise


    def _broadcast_addresses(self):
        broadcast = self._get_broadcast_address()
        if broadcast:
            return [broadcast]
        return ["255.255.255.255"]


    def _get_broadcast_address(self):
        netmask = self._get_netmask()
        if not netmask:
            return None

        network = ipaddress.IPv4Network(
            f"{self.ip}/{netmask}", strict=False
        )
        return str(network.broadcast_address)


    def _get_netmask(self):
        if fcntl is None:
            return self._get_windows_netmask()

        interface_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            for _, interface in socket.if_nameindex():
                request = struct.pack("256s", interface[:15].encode())
                try:
                    address_data = fcntl.ioctl(
                        interface_socket.fileno(), SIOCGIFADDR, request
                    )
                    interface_ip = socket.inet_ntoa(address_data[20:24])
                    if interface_ip != self.ip:
                        continue

                    netmask_data = fcntl.ioctl(
                        interface_socket.fileno(), SIOCGIFNETMASK, request
                    )
                    return socket.inet_ntoa(netmask_data[20:24])
                except OSError:
                    continue
        finally:
            interface_socket.close()

        return None


    def _get_windows_netmask(self):
        import ctypes
        from ctypes import wintypes

        class IpAddressString(ctypes.Structure):
            pass

        ip_address_pointer = ctypes.POINTER(IpAddressString)
        IpAddressString._fields_ = [
            ("next", ip_address_pointer),
            ("ip_address", ctypes.c_char * 16),
            ("ip_mask", ctypes.c_char * 16),
            ("context", wintypes.DWORD),
        ]

        class AdapterInfo(ctypes.Structure):
            pass

        adapter_pointer = ctypes.POINTER(AdapterInfo)
        AdapterInfo._fields_ = [
            ("next", adapter_pointer),
            ("combo_index", wintypes.DWORD),
            ("adapter_name", ctypes.c_char * 260),
            ("description", ctypes.c_char * 132),
            ("address_length", wintypes.UINT),
            ("address", ctypes.c_ubyte * 8),
            ("index", wintypes.DWORD),
            ("type", wintypes.UINT),
            ("dhcp_enabled", wintypes.UINT),
            ("current_ip_address", ip_address_pointer),
            ("ip_address_list", IpAddressString),
        ]

        size = wintypes.ULONG()
        get_adapters_info = ctypes.windll.iphlpapi.GetAdaptersInfo
        get_adapters_info.argtypes = [adapter_pointer, ctypes.POINTER(wintypes.ULONG)]
        get_adapters_info.restype = wintypes.ULONG
        result = get_adapters_info(None, ctypes.byref(size))
        if result not in (0, 111) or size.value == 0:
            return None

        buffer = ctypes.create_string_buffer(size.value)
        adapter = ctypes.cast(buffer, adapter_pointer)

        if get_adapters_info(adapter, ctypes.byref(size)) != 0:
            return None

        while adapter:
            address = adapter.contents.ip_address_list
            while True:
                ip = address.ip_address.decode()
                if ip == self.ip:
                    return address.ip_mask.decode()
                if not address.next:
                    break
                address = address.next.contents
            adapter = adapter.contents.next

        return None


    def _packet_ip(self, packet, source_ip):
        if source_ip and not source_ip.startswith("127."):
            return source_ip
        return packet["ip"]


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
            elif command == "logs":
                ui.show_logs(self.nickname)
            elif command == "token adicionar":
                self._send_to_successor(build_token(), "TOKEN")
                ui.show_message("TOKEN enviado. Detalhes no log.")
            elif command == "token remover":
                self.remove_token = True
                ui.log(self.nickname, "o proximo TOKEN sera removido")
                ui.show_message("O próximo TOKEN será removido.")
            elif command.startswith("mensagem "):
                self._add_message(command)
            else:
                ui.log(self.nickname, "comando desconhecido")
                ui.show_message("Comando desconhecido. Digite ajuda.")

        self.running = False
        self.socket.close()


    def _add_message(self, command):
        parts = command.split(" ", 2)
        if len(parts) < 3:
            ui.log(self.nickname, "uso: mensagem <destino|BROADCAST> <texto>")
            ui.show_message("Uso: mensagem <destino|BROADCAST> <texto>")
            return

        if self.queue.full():
            ui.log(self.nickname, "fila cheia")
            ui.show_message("Fila cheia.")
            return

        self.queue.put({
            "destination": parts[1],
            "message": parts[2],
            "tries": 0,
        })
        ui.log(self.nickname, f"mensagem para {parts[1]} adicionada")
        ui.show_message(f"Mensagem para {parts[1]} adicionada à fila.")


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
