from datetime import datetime
from pathlib import Path
from threading import Lock


LOG_DIRECTORY = Path("logs")
LOG_LOCK = Lock()


def _log_path(nickname: str) -> Path:
    filename = "".join(
        character if character.isalnum() or character in "-_" else "_"
        for character in nickname
    )
    return LOG_DIRECTORY / f"{filename or 'node'}.log"


def start_log(nickname: str) -> Path:
    path = _log_path(nickname)
    LOG_DIRECTORY.mkdir(exist_ok=True)
    path.write_text("", encoding="utf-8")
    return path.resolve()


def log(nickname: str, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"[{timestamp}] [{nickname}] {message}\n"
    with LOG_LOCK:
        with _log_path(nickname).open("a", encoding="utf-8") as log_file:
            log_file.write(line)


def show_message(message: str, next: bool = False) -> None:
    print(message, flush=True)
    if not next:
        print("> ", end="", flush=True)


def show_logs(nickname: str, line_count: int = 20) -> None:
    path = _log_path(nickname)
    if not path.exists():
        print("Nenhum log registrado.")
        return

    with LOG_LOCK:
        lines = path.read_text(encoding="utf-8").splitlines()

    print("\n".join(lines[-line_count:]) or "Nenhum log registrado.")


def show_help() -> None:
    print(
        "\nComandos:\n"
        "  mensagem <destino|BROADCAST> <texto>\n"
        "  token adicionar\n"
        "  token remover\n"
        "  topologia\n"
        "  fila\n"
        "  logs\n"
        "  ajuda\n"
        "  sair\n"
    )


def read_command() -> str:
    return input("> ").strip()


def show_topology(machines: dict[str, str]) -> None:
    ordered = sorted(machines)
    ring = " -> ".join(ordered + ordered[:1])
    print(f"Topologia: {ring}")
    for nickname in ordered:
        print(f"  {nickname}: {machines[nickname]}")


def show_queue(messages) -> None:
    if not messages:
        print("Fila vazia")
        return

    for index, item in enumerate(messages, start=1):
        print(
            f"  {index}. {item['destination']}: {item['message']} "
            f"(envios={item['tries']})"
        )
