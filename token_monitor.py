import time

from packets import build_token
import ui


def monitor_token(node):
    while node.running:
        time.sleep(0.5)
        if not node.controls_token or node.last_token_time is None:
            continue

        if time.time() - node.last_token_time > node.token_timeout:
            ui.log(node.nickname, "TOKEN perdido; gerando novo")
            node.last_token_time = time.time()
            node._send_to_successor(build_token(), "TOKEN")
