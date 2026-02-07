import queue
import threading
import time

import webview

from start_server import run_server


def _start_server(port_queue):
    def _on_ready(port):
        port_queue.put(port)

    run_server(
        port=8000,
        local_only=True,
        allow_port_fallback=True,
        print_startup=False,
        on_ready=_on_ready,
    )


def main():
    port_queue = queue.Queue()
    server_thread = threading.Thread(target=_start_server, args=(port_queue,), daemon=True)
    server_thread.start()

    port = None
    for _ in range(50):
        try:
            port = port_queue.get_nowait()
            break
        except queue.Empty:
            time.sleep(0.1)

    if not port:
        raise RuntimeError("Server did not start in time.")

    url = f"http://127.0.0.1:{port}/frontend/"
    webview.create_window("SLG Monitor 3.0", url)
    webview.start()


if __name__ == "__main__":
    main()
