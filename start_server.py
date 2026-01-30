#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
静态资源服务器：提供 frontend/、output/、data 等目录，供前端访问。
带 CORS 头，避免本地开发时跨域问题。

安全说明：仅支持 GET/HEAD 只读访问，禁止 PUT/POST/DELETE 等写操作，不会修改或删除本地任何文件。
同网共享：绑定 0.0.0.0 后，同事可通过 http://<本机IP>:端口/frontend/ 访问。
"""
import argparse
import http.server
import json
import socketserver
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_PORT = 8000
BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"
WEEKS_INDEX_PATH = FRONTEND_DIR / "data" / "weeks_index.json"
THEME_STYLE_MAPPING_PATH = FRONTEND_DIR / "data" / "product_theme_style_mapping.json"
INDEX_HTML_PATH = FRONTEND_DIR / "index.html"

# 多线程：每个请求在独立线程中处理，充分利用 M 系列多核，避免视频代理/大文件阻塞其它请求
class ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    request_queue_size = 32

# 视频代理只允许转发到以下主机（含 S3 区域端点），避免被滥用
def _is_allowed_video_host(netloc):
    n = (netloc or "").lower()
    return "x-ad-assets" in n and "amazonaws" in n


def get_lan_ips():
    """获取本机局域网 IP 列表，便于同网同事访问。"""
    out = []
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        try:
            s.connect(("8.8.8.8", 80))
            out.append(s.getsockname()[0])
        except Exception:
            pass
        s.close()
    except Exception:
        pass
    if not out:
        try:
            import subprocess
            r = subprocess.run(
                ["ifconfig"] if sys.platform == "darwin" else ["ip", "addr"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if r.returncode == 0 and r.stdout:
                for line in r.stdout.splitlines():
                    if "inet " in line and "127.0.0.1" not in line:
                        parts = line.strip().split()
                        for i, p in enumerate(parts):
                            if p == "inet" and i + 1 < len(parts):
                                ip = parts[i + 1].split("%")[0].split("/")[0]
                                if ip and not ip.startswith("127."):
                                    out.append(ip)
                                break
        except Exception:
            pass
    return out


class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    """只读静态服务器：禁止 PUT/POST/DELETE，仅允许 GET/HEAD，不修改、不删除任何本地文件。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def _fix_typo_path(self):
        """将常见拼写错误 /fronted 转为 /frontend/；对 /favicon.ico 标记为不落盘，返回 204 减少 404 刷屏。"""
        raw = self.path or ""
        p = raw.split("?")[0].rstrip("/")
        if p == "/fronted":
            self.path = "/frontend/" + ("?" + raw.split("?", 1)[1] if "?" in raw else "")
        self._send_no_content = p == "/favicon.ico"

    def _serve_frontend_index_with_weeks(self):
        """访问 /frontend 或 /frontend/ 时返回 index.html，并注入 weeks_index.json，避免前端 fetch 失败导致侧栏空白。"""
        raw = (self.path or "").split("?")[0].rstrip("/")
        if raw != "/frontend" and raw != "/frontend/":
            return False
        if not INDEX_HTML_PATH.exists():
            return False
        try:
            html = INDEX_HTML_PATH.read_text(encoding="utf-8")
        except Exception:
            return False
        # 注入周索引（必须在 app.js 之前执行，供 loadWeeksIndex 使用）
        inj_scripts = []
        if WEEKS_INDEX_PATH.exists():
            try:
                data = json.loads(WEEKS_INDEX_PATH.read_text(encoding="utf-8"))
                inj_scripts.append("window.__WEEKS_INDEX__=" + json.dumps(data, ensure_ascii=False))
            except Exception:
                pass
        # 注入题材/画风映射（只从 mapping/产品归属.xlsx 一张表取，供产品详情页统一显示）
        if THEME_STYLE_MAPPING_PATH.exists():
            try:
                mapping = json.loads(THEME_STYLE_MAPPING_PATH.read_text(encoding="utf-8"))
                inj_scripts.append("window.__PRODUCT_THEME_STYLE_MAPPING__=" + json.dumps(mapping, ensure_ascii=False))
            except Exception:
                pass
        if inj_scripts:
            inj = "<script>" + ";".join(inj_scripts) + "</script>\n  "
            if inj.strip() not in html:
                html = html.replace('<script src="js/app.js"></script>', inj + '<script src="js/app.js"></script>')
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))
        return True

    def _handle_video_proxy(self):
        """同网共享时：另一台电脑通过本机代理拉取外部视频，避免对方无法直连 CDN。"""
        raw = self.path or ""
        if not raw.startswith("/video-proxy?"):
            return False
        qs = raw.split("?", 1)[-1]
        params = urllib.parse.parse_qs(qs)
        urls = params.get("url", [])
        if not urls:
            self.send_error(400, "Missing url parameter")
            return True
        target_url = urls[0].strip()
        try:
            parsed = urllib.parse.urlparse(target_url)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                self.send_error(400, "Invalid url")
                return True
            if not _is_allowed_video_host(parsed.netloc):
                self.send_error(403, "Proxy only allows known CDN hosts")
                return True
            req = urllib.request.Request(target_url, headers={"User-Agent": "SLG-Monitor-Video-Proxy/1"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                self.send_response(200)
                self.send_header("Content-Type", resp.headers.get("Content-Type", "video/mp4"))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "public, max-age=3600")
                self.end_headers()
                chunk_size = 512 * 1024
                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        return True
        except (BrokenPipeError, ConnectionResetError):
            return True
        except Exception as e:
            try:
                self.send_error(502, "Proxy fetch failed: " + str(e)[:50])
            except (BrokenPipeError, ConnectionResetError):
                pass
        return True

    def do_GET(self):
        self._send_no_content = False
        self._fix_typo_path()
        if getattr(self, "_send_no_content", False):
            self.send_response(204)
            self.end_headers()
            return
        if self._handle_video_proxy():
            return
        if self._serve_frontend_index_with_weeks():
            return
        super().do_GET()

    def do_HEAD(self):
        self._send_no_content = False
        self._fix_typo_path()
        if getattr(self, "_send_no_content", False):
            self.send_response(204)
            self.end_headers()
            return
        super().do_HEAD()

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        super().end_headers()

    def do_PUT(self):
        self.send_error(405, "Method Not Allowed (read-only server)")
        self.log_message("BLOCKED PUT %s", self.path)

    def do_POST(self):
        self.send_error(405, "Method Not Allowed (read-only server)")
        self.log_message("BLOCKED POST %s", self.path)

    def do_DELETE(self):
        self.send_error(405, "Method Not Allowed (read-only server)")
        self.log_message("BLOCKED DELETE %s", self.path)

    def do_PATCH(self):
        self.send_error(405, "Method Not Allowed (read-only server)")
        self.log_message("BLOCKED PATCH %s", self.path)

    def log_message(self, format, *args):
        print(format % args, flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="SLG Monitor 静态资源服务（只读，同网可共享）",
        epilog="同网同事访问：用下面打印的「本机 IP」替换 localhost，如 http://192.168.1.100:8000/frontend/",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"监听端口，默认 {DEFAULT_PORT}。若端口被占用可指定其他端口如 8001",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="仅监听 127.0.0.1，不允许同网访问（默认监听所有网卡，允许同网访问）",
    )
    args = parser.parse_args()
    bind_host = "127.0.0.1" if args.local_only else ""
    # 自动切换端口：首选端口被占用时依次尝试下一端口（如 8000 -> 8001）
    ports_to_try = [args.port, args.port + 1]

    os.chdir(BASE_DIR)
    ThreadedHTTPServer.allow_reuse_address = True
    httpd = None
    used_port = None
    for port in ports_to_try:
        try:
            httpd = ThreadedHTTPServer((bind_host, port), CORSRequestHandler)
            used_port = port
            break
        except OSError as e:
            if e.errno == 48:  # Address already in use
                if port == ports_to_try[-1]:
                    print(f"端口 {ports_to_try[0]}、{ports_to_try[1]} 均已被占用，请先结束占用进程或指定其他端口。", flush=True)
                    sys.exit(1)
                print(f"端口 {port} 已被占用，尝试下一端口 {port + 1} …", flush=True)
            else:
                print(f"启动失败: {e}", flush=True)
                sys.exit(1)

    port = used_port
    try:
        with httpd:
            print("=" * 60, flush=True)
            print("SLG Monitor 静态资源服务（多线程，只读）", flush=True)
            print("=" * 60, flush=True)
            if port != args.port:
                print(f"（首选端口 {args.port} 被占用，已使用端口 {port}）", flush=True)
            print(f"本机访问:   http://localhost:{port}/frontend/", flush=True)
            if not args.local_only:
                lan_ips = get_lan_ips()
                for ip in lan_ips:
                    print(f"同网访问:   http://{ip}:{port}/frontend/", flush=True)
                if not lan_ips:
                    print("同网访问:   请在本机「系统设置 → 网络」中查看本机 IP，再使用 http://<本机IP>:%d/frontend/" % port, flush=True)
            print("按 Ctrl+C 或 PyCharm 停止按钮结束服务", flush=True)
            print("=" * 60, flush=True)
            try:
                httpd.serve_forever()
            except KeyboardInterrupt:
                print("\n正在关闭服务器…", flush=True)
                httpd.shutdown()
    except OSError as e:
        print(f"启动失败: {e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
