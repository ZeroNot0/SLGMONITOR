import base64
import os
import queue
import threading
import time
import sys
from pathlib import Path

import webview

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

if hasattr(sys, "_MEIPASS"):
    exe_dir = Path(sys.executable).resolve().parent
    data_dir = exe_dir / "SLGMonitorData"
    os.environ.setdefault("SLG_MONITOR_DATA_DIR", str(data_dir))

from server.start_server import run_server


class AppApi:
    def __init__(self, window=None):
        self._window = window

    def _choose_save_path(self, default_name, file_types):
        filename = str(default_name or "data")
        downloads_dir = Path.home() / "Downloads"
        directory = str(downloads_dir) if downloads_dir.exists() else ""
        dialog_window = self._window if self._window is not None else webview
        dialog_type = getattr(webview, "SAVE_FILE_DIALOG", None)
        if dialog_type is None:
            dialog_type = getattr(webview, "SAVE_DIALOG", "save")
        types = file_types or [("All Files (*.*)", "*.*")]
        try:
            result = dialog_window.create_file_dialog(
                dialog_type,
                directory=directory,
                save_filename=filename,
                file_types=types,
            )
        except Exception:
            result = dialog_window.create_file_dialog(
                dialog_type,
                directory=directory,
                save_filename=filename,
            )
        if not result:
            return None
        if isinstance(result, (list, tuple)):
            if not result:
                return None
            return Path(result[0])
        return Path(result)

    def save_file(self, default_name, content):
        try:
            path = self._choose_save_path(
                default_name or "data.csv",
                [("CSV Files (*.csv)", "*.csv"), ("All Files (*.*)", "*.*")],
            )
            if not path:
                return {"ok": False, "cancelled": True}
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(str(content), encoding="utf-8-sig")
            return {"ok": True, "path": str(path)}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def save_bytes(self, default_name, data_base64):
        try:
            filename = default_name or "data.bin"
            path = self._choose_save_path(
                filename,
                [("All Files (*.*)", "*.*")],
            )
            if not path:
                return {"ok": False, "cancelled": True}
            raw = base64.b64decode(data_base64 or "")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            return {"ok": True, "path": str(path)}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def save_xlsx(self, default_name, headers, rows):
        try:
            from openpyxl import Workbook

            filename = default_name or "data.xlsx"
            if not filename.lower().endswith(".xlsx"):
                filename += ".xlsx"
            path = self._choose_save_path(
                filename,
                [("Excel Files (*.xlsx)", "*.xlsx"), ("All Files (*.*)", "*.*")],
            )
            if not path:
                return {"ok": False, "cancelled": True}
            wb = Workbook()
            ws = wb.active
            if headers:
                ws.append(["" if h is None else str(h) for h in headers])
            for row in rows or []:
                ws.append(["" if v is None else v for v in row])
            path.parent.mkdir(parents=True, exist_ok=True)
            wb.save(str(path))
            return {"ok": True, "path": str(path)}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}


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
    api = AppApi()
    window = webview.create_window("SLG Monitor 3.0", url, js_api=api)
    api._window = window
    try:
        webview.start(gui="edgechromium")
    except Exception:
        webview.start()


if __name__ == "__main__":
    main()
