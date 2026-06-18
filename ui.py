def log(nickname: str, message: str) -> None:
    print(f"[{nickname}] {message}", flush=True)


def show_help() -> None:
    print(
        "\nComandos:\n"
        "  mensagem <destino|BROADCAST> <texto>\n"
        "  token adicionar\n"
        "  token remover\n"
        "  topologia\n"
        "  fila\n"
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
