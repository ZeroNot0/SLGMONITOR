#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
静态资源服务器：提供 frontend/、output/、data 等目录，供前端访问。
带 CORS 头，避免本地开发时跨域问题。
数据维护：POST /api/maintenance/phase1 接收 13 个 CSV + year/week_tag，落盘后执行流水线第一步并更新前端。
同网共享：绑定 0.0.0.0 后，同事可通过 http://<本机IP>:端口/frontend/ 访问。
"""
import argparse
import hashlib
import http.server
import io
import json
import re
import secrets
import socketserver
import os
import sys
import threading
import urllib.parse
import urllib.request
from pathlib import Path


def _parse_multipart_form_data(body: bytes, content_type: str):
    """解析 multipart/form-data，返回 (fields_dict, files_list)。
    files_list 中每项为 (filename_or_none, bytes_content)。
    不依赖 cgi.FieldStorage，避免多文件时 .file 为空导致 No valid CSV。
    """
    # 取 boundary，如 boundary=----WebKitFormBoundary...
    m = re.search(r'boundary=([^;\s]+)', content_type, re.I)
    boundary = m.group(1).strip('"').encode("ascii") if m else None
    if not boundary:
        return {}, []
    # 按 boundary 分割，去掉首尾空块
    parts = body.split(b"--" + boundary)
    fields = {}
    files_list = []
    for part in parts:
        part = part.strip()
        if not part or part == b"--":
            continue
        # 第一段：\r\n\r\n 前为头，后为正文
        if b"\r\n\r\n" in part:
            raw_headers, rest = part.split(b"\r\n\r\n", 1)
            # 正文末尾可能带 \r\n
            content = rest.rstrip(b"\r\n") if rest else b""
        else:
            continue
        # 解析 Content-Disposition
        disp = None
        for line in raw_headers.split(b"\r\n"):
            if line.lower().startswith(b"content-disposition:"):
                disp = line.decode("utf-8", errors="replace")
                break
        if not disp:
            continue
        name_m = re.search(r'name="([^"]+)"', disp, re.I)
        if not name_m:
            continue
        name = name_m.group(1)
        filename_m = re.search(r'filename="([^"]*)"', disp, re.I)
        filename = filename_m.group(1).strip() if filename_m else None
        if filename:
            files_list.append((filename, content))
        else:
            fields[name] = content.decode("utf-8", errors="replace").strip()
    return fields, files_list


def _excel_to_headers_rows(path: Path) -> tuple:
    """读取 Excel 第一 sheet，返回 (headers: list, rows: list of list)。文件不存在或读失败返回 ([], [])。"""
    if not path or not path.is_file():
        return [], []
    try:
        import pandas as pd
        df = pd.read_excel(path)
    except Exception:
        return [], []
    if df.empty:
        return [], []
    headers = [str(c).strip() if c is not None else "" for c in df.columns]
    rows = []
    for _, r in df.iterrows():
        row = []
        for v in r:
            if pd.isna(v):
                row.append("")
            elif isinstance(v, (int, float)) and not isinstance(v, bool):
                row.append(int(v) if v == int(v) else v)
            else:
                row.append(str(v).strip())
        rows.append(row)
    return headers, rows


DEFAULT_PORT = 8000
BASE_DIR = Path(__file__).resolve().parent
# 设为 True 时禁止写操作（PUT/POST/DELETE/PATCH 返回 405）；做权限管理后可改回 True
READ_ONLY_SERVER = False
FRONTEND_DIR = BASE_DIR / "frontend"
WEEKS_INDEX_PATH = FRONTEND_DIR / "data" / "weeks_index.json"
MAPPING_DIR = BASE_DIR / "mapping"
LABELS_DIR = BASE_DIR / "labels"
# 数据底表 API 名称 -> Excel 路径（前端 产品总表 / 新产品监测表 直接读 data 下 JSON）
BASETABLE_SOURCES = {
    "product_mapping": MAPPING_DIR / "产品归属.xlsx",
    "company_mapping": MAPPING_DIR / "公司归属.xlsx",
    "theme_label": LABELS_DIR / "题材标签表.xlsx",
    "gameplay_label": LABELS_DIR / "玩法标签表.xlsx",
    "art_style_label": LABELS_DIR / "画风标签表.xlsx",
}
THEME_STYLE_MAPPING_PATH = FRONTEND_DIR / "data" / "product_theme_style_mapping.json"
INDEX_HTML_PATH = FRONTEND_DIR / "index.html"

# 登录与权限：用户表路径、session 存储、Cookie 名
AUTH_USERS_PATH = BASE_DIR / "deploy" / "auth_users.json"
AUTH_SESSIONS = {}  # session_id -> {"username": str}
AUTH_SESSIONS_LOCK = threading.Lock()
AUTH_COOKIE_NAME = "slg_session"
AUTH_COOKIE_MAX_AGE = 7 * 24 * 3600  # 7 天


def _load_auth_users():
    """加载 deploy/auth_users.json，格式：{"users": [{"username", "salt", "hash"}]}。"""
    if not AUTH_USERS_PATH.is_file():
        return []
    try:
        data = json.loads(AUTH_USERS_PATH.read_text(encoding="utf-8"))
        return data.get("users") or []
    except Exception:
        return []


def _verify_password(username: str, password: str) -> bool:
    """校验用户名与密码。"""
    users = _load_auth_users()
    for u in users:
        if (u.get("username") or "").strip() == username.strip():
            salt = (u.get("salt") or "").encode("utf-8")
            h = (u.get("hash") or "").strip()
            if not h:
                return False
            computed = hashlib.sha256(salt + password.encode("utf-8")).hexdigest()
            return secrets.compare_digest(computed, h)
    return False


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

    def _handle_maintenance_download(self):
        """GET /api/maintenance/download?year=2026&week=0105-0111：返回 output/{年}/{周}_SLG数据监测表.xlsx 供下载。"""
        raw = self.path or ""
        if not raw.startswith("/api/maintenance/download"):
            return False
        qs = raw.split("?", 1)[-1] if "?" in raw else ""
        params = urllib.parse.parse_qs(qs)
        year = (params.get("year") or [None])[0]
        week = (params.get("week") or [None])[0]
        if not year or not week:
            self.send_error(400, "Missing year or week")
            return True
        year = str(year).strip()
        week = urllib.parse.unquote(str(week).strip())
        if not year.isdigit() or len(year) != 4:
            self.send_error(400, "year must be 4 digits")
            return True
        out_path = BASE_DIR / "output" / year / ("%s_SLG数据监测表.xlsx" % week)
        if not out_path.is_file():
            self.send_error(404, "File not found: %s" % out_path.name)
            return True
        try:
            with open(out_path, "rb") as f:
                data = f.read()
        except Exception as e:
            self.send_error(500, str(e)[:80])
            return True
        # 文件名含中文，HTTP 头仅支持 latin-1，用 RFC 5987 filename*=UTF-8'' 编码
        filename_utf8 = ("%s_SLG数据监测表.xlsx" % week).encode("utf-8")
        filename_ascii = "%s_SLG_data_monitor.xlsx" % week
        disp_value = "attachment; filename=\"%s\"; filename*=UTF-8''%s" % (
            filename_ascii,
            urllib.parse.quote(filename_utf8.decode("utf-8"), safe=""),
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        self.send_header("Content-Disposition", disp_value)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        return True

    def _handle_basetable_metrics_total(self):
        """GET /api/basetable/metrics_total?year=2026&week=0112-0118&limit=1000&q=搜索词：产品总表分页+搜索，返回 {headers, rows, total}，避免前端一次拉取 7 万行。"""
        raw = self.path or ""
        if not raw.startswith("/api/basetable/metrics_total"):
            return False
        qs = raw.split("?", 1)[-1] if "?" in raw else ""
        params = urllib.parse.parse_qs(qs)
        year = (params.get("year") or [""])[0].strip()
        week = (params.get("week") or [""])[0].strip()
        if not year or not week or not year.isdigit() or len(year) != 4:
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "year and week required"}, ensure_ascii=False).encode("utf-8"))
            return True
        try:
            limit = int((params.get("limit") or [1000])[0])
        except (TypeError, ValueError):
            limit = 1000
        limit = max(1, min(limit, 50000))
        q = (params.get("q") or [""])[0].strip()
        json_path = FRONTEND_DIR / "data" / year / week / "metrics_total.json"
        if not json_path.is_file():
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"headers": [], "rows": [], "total": 0}, ensure_ascii=False).encode("utf-8"))
            return True
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"headers": [], "rows": [], "total": 0}, ensure_ascii=False).encode("utf-8"))
            return True
        headers = data.get("headers") or []
        rows = data.get("rows") or []
        if q:
            q_lower = q.lower()
            rows = [r for r in rows if any(str(c or "").lower().find(q_lower) >= 0 for c in (r or []))]
        total = len(rows)
        rows = rows[:limit]
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"headers": headers, "rows": rows, "total": total}, ensure_ascii=False).encode("utf-8"))
        return True

    def _handle_basetable_metrics_total_product_names(self):
        """GET /api/basetable/metrics_total_product_names?year=2026&week=0112-0118：返回该周产品总表中所有产品名及 Unified ID 映射，供上线新游「是否在总表中存在」全量匹配（不受 limit 限制）。"""
        raw = self.path or ""
        if not raw.startswith("/api/basetable/metrics_total_product_names"):
            return False
        qs = raw.split("?", 1)[-1] if "?" in raw else ""
        params = urllib.parse.parse_qs(qs)
        year = (params.get("year") or [""])[0].strip()
        week = (params.get("week") or [""])[0].strip()
        if not year or not week or not year.isdigit() or len(year) != 4:
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "year and week required"}, ensure_ascii=False).encode("utf-8"))
            return True
        json_path = FRONTEND_DIR / "data" / year / week / "metrics_total.json"
        if not json_path.is_file():
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"productNames": [], "nameToUnifiedId": {}}, ensure_ascii=False).encode("utf-8"))
            return True
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"productNames": [], "nameToUnifiedId": {}}, ensure_ascii=False).encode("utf-8"))
            return True
        headers = data.get("headers") or []
        rows = data.get("rows") or []
        name_idx = next((i for i, h in enumerate(headers) if (h or "").strip() == "Unified Name"), -1)
        id_idx = next((i for i, h in enumerate(headers) if (h or "").strip() == "Unified ID"), -1)
        product_names = []
        name_to_id = {}
        seen = set()
        for row in rows:
            if not row or name_idx < 0:
                continue
            raw_name = row[name_idx] if name_idx < len(row) else None
            name_val = (str(raw_name).strip() if raw_name is not None and raw_name != "" else "")
            if not name_val:
                continue
            if name_val not in seen:
                seen.add(name_val)
                product_names.append(name_val)
            if id_idx >= 0 and name_val and name_val not in name_to_id:
                raw_id = row[id_idx] if id_idx < len(row) else None
                id_val = (str(raw_id).strip() if raw_id is not None and raw_id != "" else "")
                if id_val:
                    name_to_id[name_val] = id_val
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"productNames": product_names, "nameToUnifiedId": name_to_id}, ensure_ascii=False).encode("utf-8"))
        return True

    def _handle_basetable(self):
        """GET /api/basetable?name=product_mapping|company_mapping|theme_label|gameplay_label|art_style_label：返回底表 JSON {headers, rows}。"""
        raw = self.path or ""
        if not raw.startswith("/api/basetable"):
            return False
        qs = raw.split("?", 1)[-1] if "?" in raw else ""
        params = urllib.parse.parse_qs(qs)
        name = (params.get("name") or [""])[0].strip()
        if name not in BASETABLE_SOURCES:
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "missing or invalid name"}, ensure_ascii=False).encode("utf-8"))
            return True
        path = BASETABLE_SOURCES[name]
        headers, rows = _excel_to_headers_rows(path)
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"headers": headers, "rows": rows}, ensure_ascii=False).encode("utf-8"))
        return True

    def _get_cookie(self, name: str):
        """从请求头 Cookie 中解析指定名称的值。"""
        cookie = self.headers.get("Cookie") or ""
        for part in cookie.split(";"):
            part = part.strip()
            if part.startswith(name + "="):
                return urllib.parse.unquote(part[len(name) + 1 :].strip())
        return None

    def _get_session_username(self):
        """从 Cookie 中读取 session_id，校验后返回用户名，无效返回 None。"""
        sid = self._get_cookie(AUTH_COOKIE_NAME)
        if not sid:
            return None
        with AUTH_SESSIONS_LOCK:
            info = AUTH_SESSIONS.get(sid)
        return (info.get("username") if info else None) or None

    def _require_auth_for_api(self):
        """对需要登录的 /api/* 校验 session；公开接口返回 True。若未登录则发送 401 并返回 False。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path == "/api/auth/check" or path == "/api/auth/login":
            return True
        if not path.startswith("/api/"):
            return True
        if self._get_session_username():
            return True
        self.send_response(401)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": False, "message": "未登录或登录已过期"}, ensure_ascii=False).encode("utf-8"))
        return False

    def _handle_auth_check(self):
        """GET /api/auth/check：校验当前 Cookie 对应 session，返回 {ok, username} 或 401。"""
        raw = (self.path or "").split("?")[0].rstrip("/")
        if raw != "/api/auth/check":
            return False
        username = self._get_session_username()
        if not username:
            self.send_response(401)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False}, ensure_ascii=False).encode("utf-8"))
            return True
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True, "username": username}, ensure_ascii=False).encode("utf-8"))
        return True

    def _handle_auth_login(self):
        """POST /api/auth/login：Body JSON {username, password}，校验通过则设置 session Cookie。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path != "/api/auth/login":
            return False
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "缺少请求体"}, ensure_ascii=False).encode("utf-8"))
                return True
            body = self.rfile.read(length)
            data = json.loads(body.decode("utf-8"))
            username = (data.get("username") or "").strip()
            password = data.get("password") or ""
            if not username:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "请输入用户名"}, ensure_ascii=False).encode("utf-8"))
                return True
            if not _verify_password(username, password):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "用户名或密码错误"}, ensure_ascii=False).encode("utf-8"))
                return True
            session_id = secrets.token_urlsafe(32)
            with AUTH_SESSIONS_LOCK:
                AUTH_SESSIONS[session_id] = {"username": username}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header(
                "Set-Cookie",
                "%s=%s; Path=/; Max-Age=%d; HttpOnly; SameSite=Lax"
                % (AUTH_COOKIE_NAME, session_id, AUTH_COOKIE_MAX_AGE),
            )
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "username": username}, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("auth/login error: %s", e)
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            return True

    def _handle_auth_logout(self):
        """POST /api/auth/logout：清除 session 并删除 Cookie。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path != "/api/auth/logout":
            return False
        sid = self._get_cookie(AUTH_COOKIE_NAME)
        if sid:
            with AUTH_SESSIONS_LOCK:
                AUTH_SESSIONS.pop(sid, None)
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Set-Cookie", "%s=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax" % AUTH_COOKIE_NAME)
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8"))
        return True

    def do_GET(self):
        self._send_no_content = False
        self._fix_typo_path()
        if getattr(self, "_send_no_content", False):
            self.send_response(204)
            self.end_headers()
            return
        if self._handle_auth_check():
            return
        if not self._require_auth_for_api():
            return
        if self._handle_video_proxy():
            return
        if self._handle_maintenance_download():
            return
        if self._handle_basetable_metrics_total():
            return
        if self._handle_basetable_metrics_total_product_names():
            return
        if self._handle_basetable():
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

    def do_OPTIONS(self):
        """CORS 预检：允许对 maintenance、auth 接口的 POST，避免浏览器报 Method Not Allowed。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path in ("/api/maintenance/phase1", "/api/maintenance/phase2_1", "/api/maintenance/phase2_2", "/api/maintenance/mapping_update", "/api/maintenance/newproducts_update", "/api/maintenance/add_to_product_mapping", "/api/auth/login", "/api/auth/logout"):
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Allow", "POST, OPTIONS")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Allow", "GET, HEAD, OPTIONS")
        self.end_headers()

    def do_PUT(self):
        if READ_ONLY_SERVER:
            self.send_error(405, "Method Not Allowed (read-only server)")
            self.log_message("BLOCKED PUT %s", self.path)
        else:
            self.send_error(404, "Not Found")

    def _handle_maintenance_phase1(self):
        """POST /api/maintenance/phase1：接收 year、week_tag、files，落盘到 raw_csv/{year}/{week_tag}/ 后执行第一步+前端更新。"""
        try:
            ctype = self.headers.get("Content-Type", "")
            if not ctype.startswith("multipart/form-data"):
                self.send_error(400, "Content-Type must be multipart/form-data")
                return True
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                self.send_error(400, "Missing Content-Length")
                return True
            body = self.rfile.read(length)
            fields, files_list = _parse_multipart_form_data(body, ctype)
            year_val = (fields.get("year") or "").strip()
            week_val = (fields.get("week_tag") or "").strip()
            if not year_val or not week_val:
                self.send_error(400, "Missing year or week_tag")
                return True
            if not year_val.isdigit() or len(year_val) != 4:
                self.send_error(400, "year must be 4 digits")
                return True
            if not re.match(r"^\d{4}-\d{4}$", week_val):
                self.send_error(400, "week_tag must be like 0119-0125")
                return True
            year = int(year_val)
            if not files_list:
                self.send_error(400, "Missing files")
                return True
            raw_dir = BASE_DIR / "raw_csv" / str(year) / week_val
            raw_dir.mkdir(parents=True, exist_ok=True)
            saved = 0
            for idx, (filename, content) in enumerate(files_list):
                if not content:
                    continue
                name = (os.path.basename(filename) if filename else "").strip() or ("file_%d.csv" % idx)
                if not name.lower().endswith(".csv"):
                    name = name + ".csv"
                out_path = raw_dir / name
                with open(out_path, "wb") as f:
                    f.write(content)
                saved += 1
            if saved == 0:
                self.send_error(400, "No valid CSV file uploaded")
                return True
            if saved < 13:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "message": "上传文件数不足 13 个，当前仅 %d 个，无法处理。请补全后重试。" % saved
                }, ensure_ascii=False).encode("utf-8"))
                return True
            from run_full_pipeline import ensure_raw_csv_for_step1, run_phase1, run_phase3
            ensure_raw_csv_for_step1(year, week_val)
            if not run_phase1(week_val, year):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "第一步流水线执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            if not run_phase3(week_val, year):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "前端更新执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            # 第一步完成后显式将 metrics_total 转为 JSON，供数据底表「产品总表」展示
            metrics_xlsx = BASE_DIR / "intermediate" / str(year) / week_val / "metrics_total.xlsx"
            if metrics_xlsx.is_file():
                try:
                    from run_full_pipeline import run_frontend_script
                    run_frontend_script("convert_metrics_to_json.py", year=year, week_tag=week_val)
                except Exception:
                    pass
            out_excel = BASE_DIR / "output" / str(year) / ("%s_SLG数据监测表.xlsx" % week_val)
            download_url = ""
            download_name = ""
            if out_excel.exists():
                download_url = "/api/maintenance/download?year=%s&week=%s" % (year_val, urllib.parse.quote(week_val, safe=""))
                download_name = "%s_SLG数据监测表.xlsx" % week_val
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "message": "第一步执行完成，公司维度大盘数据已更新；metrics_total 已转为 JSON，数据底表「产品总表」可查看该周。",
                "downloadUrl": download_url,
                "downloadName": download_name
            }, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("maintenance/phase1 error: %s", e)
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            except Exception:
                pass
            return True

    def _handle_maintenance_phase2_1(self):
        """POST /api/maintenance/phase2_1：流水线 2.1 步，拉取目标产品分地区数据。Body JSON: year, week_tag, target, product_type, limit。"""
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                self.send_error(400, "Missing Content-Length")
                return True
            body = self.rfile.read(length)
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                self.send_error(400, "Invalid JSON")
                return True
            year_val = data.get("year")
            week_val = data.get("week_tag")
            target = (data.get("target") or "strategy").strip().lower()
            product_type = (data.get("product_type") or "both").strip().lower()
            limit = (data.get("limit") or "all").strip().lower()
            if year_val is None and week_val is None:
                self.send_error(400, "Missing year or week_tag")
                return True
            year_val = str(year_val).strip()
            week_val = str(week_val).strip()
            if not year_val.isdigit() or len(year_val) != 4:
                self.send_error(400, "year must be 4 digits")
                return True
            if not re.match(r"^\d{4}-\d{4}$", week_val):
                self.send_error(400, "week_tag must be like 0119-0125")
                return True
            if target not in ("strategy", "non_strategy"):
                target = "strategy"
            if product_type not in ("old", "new", "both"):
                product_type = "both"
            if limit not in ("top1", "top5", "top10", "top20", "all"):
                limit = "all"
            year = int(year_val)
            from run_full_pipeline import run_phase2, run_phase3
            if not run_phase2(
                week_val, year,
                fetch_country=True,
                fetch_creatives=False,
                limit=limit,
                target_source=target,
                product_type=product_type,
            ):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "2.1 步拉取地区数据执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            if not run_phase3(week_val, year):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "前端更新执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "message": "2.1 步执行完成，目标产品分地区数据已拉取并已更新前端。"
            }, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("maintenance/phase2_1 error: %s", e)
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            except Exception:
                pass
            return True

    def _handle_maintenance_phase2_2(self):
        """POST /api/maintenance/phase2_2：流水线 2.2 步，拉取目标产品创意数据。Body JSON: year, week_tag, target, product_type, limit。"""
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                self.send_error(400, "Missing Content-Length")
                return True
            body = self.rfile.read(length)
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                self.send_error(400, "Invalid JSON")
                return True
            year_val = data.get("year")
            week_val = data.get("week_tag")
            target = (data.get("target") or "strategy").strip().lower()
            product_type = (data.get("product_type") or "both").strip().lower()
            limit = (data.get("limit") or "all").strip().lower()
            if year_val is None and week_val is None:
                self.send_error(400, "Missing year or week_tag")
                return True
            year_val = str(year_val).strip()
            week_val = str(week_val).strip()
            if not year_val.isdigit() or len(year_val) != 4:
                self.send_error(400, "year must be 4 digits")
                return True
            if not re.match(r"^\d{4}-\d{4}$", week_val):
                self.send_error(400, "week_tag must be like 0119-0125")
                return True
            if target not in ("strategy", "non_strategy"):
                target = "strategy"
            if product_type not in ("old", "new", "both"):
                product_type = "both"
            if limit not in ("top1", "top5", "top10", "top20", "all"):
                limit = "all"
            year = int(year_val)
            from run_full_pipeline import run_phase2, run_phase3
            if not run_phase2(
                week_val, year,
                fetch_country=False,
                fetch_creatives=True,
                limit=limit,
                target_source=target,
                product_type=product_type,
            ):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "2.2 步拉取创意数据执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            if not run_phase3(week_val, year):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "前端更新执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "message": "2.2 步执行完成，目标产品创意数据已拉取并已更新前端。"
            }, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("maintenance/phase2_2 error: %s", e)
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            except Exception:
                pass
            return True

    def _handle_maintenance_mapping_update(self):
        """POST /api/maintenance/mapping_update：上传产品/公司归属表 Excel，校验必填列后合并进 mapping/。"""
        try:
            ctype = self.headers.get("Content-Type", "")
            if not ctype.startswith("multipart/form-data"):
                self.send_error(400, "Content-Type must be multipart/form-data")
                return True
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                self.send_error(400, "Missing Content-Length")
                return True
            body = self.rfile.read(length)
            fields, files_list = _parse_multipart_form_data(body, ctype)
            if not files_list:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "message": "请选择并上传一个 Excel 文件（.xlsx）"
                }, ensure_ascii=False).encode("utf-8"))
                return True
            filename, content = files_list[0]
            if not content or not (filename or "").lower().endswith(".xlsx"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "message": "请上传 .xlsx 格式的 Excel 文件"
                }, ensure_ascii=False).encode("utf-8"))
                return True
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            try:
                import sys
                sys.path.insert(0, str(BASE_DIR))
                from scripts.update_mapping_from_upload import run as run_mapping_update
                ok, msg = run_mapping_update(Path(tmp_path))
                if ok:
                    try:
                        from run_full_pipeline import run_frontend_script
                        run_frontend_script("convert_product_mapping_to_json.py")
                    except Exception:
                        pass
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": ok,
                "message": msg
            }, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("maintenance/mapping_update error: %s", e)
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            except Exception:
                pass
            return True

    def _handle_maintenance_newproducts_update(self):
        """POST /api/maintenance/newproducts_update：上传新产品监测表 Excel，保存到 newproducts/ 并执行 convert_newproducts_to_json 生成 frontend/data/new_products.json。"""
        try:
            ctype = self.headers.get("Content-Type", "")
            if not ctype.startswith("multipart/form-data"):
                self.send_error(400, "Content-Type must be multipart/form-data")
                return True
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                self.send_error(400, "Missing Content-Length")
                return True
            body = self.rfile.read(length)
            fields, files_list = _parse_multipart_form_data(body, ctype)
            if not files_list:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "message": "请选择并上传一个 Excel 文件（.xlsx）"
                }, ensure_ascii=False).encode("utf-8"))
                return True
            filename, content = files_list[0]
            if not content or not (filename or "").lower().endswith(".xlsx"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "message": "请上传 .xlsx 格式的 Excel 文件"
                }, ensure_ascii=False).encode("utf-8"))
                return True
            newproducts_dir = BASE_DIR / "newproducts"
            newproducts_dir.mkdir(parents=True, exist_ok=True)
            # 保存为固定文件名，便于 convert 脚本稳定读取；也可保留原文件名（脚本取第一个 xlsx）
            out_name = "新产品监测表.xlsx"
            out_path = newproducts_dir / out_name
            out_path.write_bytes(content)
            ok, msg = True, "新产品监测表已保存，正在生成上线新游 JSON…"
            try:
                sys.path.insert(0, str(BASE_DIR))
                from run_full_pipeline import run_frontend_script
                if run_frontend_script("convert_newproducts_to_json.py"):
                    msg = "新产品监测表已更新，【产品维度】-【上线新游】将展示最新数据。"
                else:
                    ok = False
                    msg = "新产品监测表已保存，但生成 new_products.json 失败，请检查 newproducts/ 下 Excel 格式或手动运行 frontend/convert_newproducts_to_json.py。"
            except Exception as e:
                ok = False
                msg = "新产品监测表已保存，但生成 JSON 时出错: %s" % e
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": ok, "message": msg}, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("maintenance/newproducts_update error: %s", e)
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            except Exception:
                pass
            return True

    def _handle_maintenance_add_to_product_mapping(self):
        """POST /api/maintenance/add_to_product_mapping：Body JSON { products: [ { 产品名, 产品归属, 题材, 画风, 发行商, 公司归属 } ] }，将新产品追加到 mapping/产品归属.xlsx（仅追加 产品归属 不在表中的行）。"""
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                self.send_error(400, "Missing body")
                return True
            body = self.rfile.read(length)
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "请求体须为 JSON"}, ensure_ascii=False).encode("utf-8"))
                return True
            products = data.get("products") or []
            if not isinstance(products, list):
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "products 须为数组"}, ensure_ascii=False).encode("utf-8"))
                return True
            try:
                import pandas as pd
            except ImportError:
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "需要 pandas、openpyxl"}, ensure_ascii=False).encode("utf-8"))
                return True
            PROD_XLSX = MAPPING_DIR / "产品归属.xlsx"
            COMP_XLSX = MAPPING_DIR / "公司归属.xlsx"
            OUT_COLS = ["产品名（实时更新中）", "Unified ID", "产品归属", "题材", "画风", "发行商", "公司归属"]
            normalized = []
            for p in products:
                if not isinstance(p, dict):
                    continue
                prod_name = (p.get("产品名") or p.get("产品名（实时更新中）") or "").strip()
                prod_belong = (p.get("产品归属") or "").strip()
                if not prod_belong:
                    continue
                theme = (p.get("题材") or "").strip()
                style = (p.get("画风") or "").strip()
                pub = (p.get("发行商") or "").strip()
                comp = (p.get("公司归属") or "").strip()
                if not comp:
                    comp = "未知"
                unified_id = (p.get("Unified ID") or p.get("unifiedId") or "").strip()
                normalized.append({
                    "产品名（实时更新中）": prod_name or prod_belong,
                    "Unified ID": unified_id,
                    "产品归属": prod_belong,
                    "题材": theme,
                    "画风": style,
                    "发行商": pub,
                    "公司归属": comp,
                })
            if not normalized:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "message": "无有效产品数据", "added": 0}, ensure_ascii=False).encode("utf-8"))
                return True
            df_new = pd.DataFrame(normalized)[OUT_COLS]
            existing_belong = set()
            df_old = pd.DataFrame()
            if PROD_XLSX.exists():
                try:
                    df_old = pd.read_excel(PROD_XLSX)
                    if not df_old.empty and "产品归属" in df_old.columns:
                        existing_belong = set(df_old["产品归属"].astype(str).str.strip().replace("nan", "").dropna().unique())
                    elif not df_old.empty and df_old.shape[1] >= 5:
                        # 按列位置：产品归属多为第 5 列（索引 4）
                        for _, row in df_old.iterrows():
                            v = str(row.iloc[4]).strip() if len(row) > 4 and pd.notna(row.iloc[4]) else ""
                            if v:
                                existing_belong.add(v)
                except Exception:
                    pass
            df_new = df_new[~df_new["产品归属"].isin(existing_belong)]
            if df_new.empty:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "message": "所选产品均已在产品归属表中，无新增", "added": 0}, ensure_ascii=False).encode("utf-8"))
                return True
            added = len(df_new)
            df_old_clean = df_old.drop(columns=["序号"], errors="ignore") if not df_old.empty else pd.DataFrame()
            if not df_old_clean.empty and all(c in df_old_clean.columns for c in OUT_COLS):
                df_merged = pd.concat([df_old_clean[OUT_COLS], df_new], ignore_index=True)
            else:
                df_merged = df_new.copy()
            df_merged.insert(0, "序号", range(1, len(df_merged) + 1))
            MAPPING_DIR.mkdir(parents=True, exist_ok=True)
            df_merged.to_excel(PROD_XLSX, index=False)
            if not df_new.empty and COMP_XLSX.exists():
                try:
                    df_comp = pd.read_excel(COMP_XLSX)
                    comp_pairs = df_new[["发行商", "公司归属"]].drop_duplicates()
                    comp_pairs = comp_pairs[comp_pairs["发行商"].notna() & (comp_pairs["发行商"].astype(str).str.strip() != "")]
                    if not comp_pairs.empty and not df_comp.empty and df_comp.shape[1] >= 2:
                        c1 = df_comp.iloc[:, 1]
                        c2 = df_comp.iloc[:, 2]
                        comp_old = pd.DataFrame({"发行商": c1, "公司归属": c2}).drop_duplicates()
                        comp_pairs = pd.concat([comp_old, comp_pairs]).drop_duplicates(subset=["发行商"], keep="last")
                        comp_pairs.insert(0, "序号", range(1, len(comp_pairs) + 1))
                        comp_pairs.to_excel(COMP_XLSX, index=False)
                except Exception:
                    pass
            try:
                sys.path.insert(0, str(BASE_DIR))
                from run_full_pipeline import run_frontend_script
                run_frontend_script("convert_product_mapping_to_json.py")
            except Exception:
                pass
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "message": "已成功将 %d 条新产品加入产品归属表" % added, "added": added}, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("maintenance/add_to_product_mapping error: %s", e)
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            except Exception:
                pass
            return True

    def do_POST(self):
        path = (self.path or "").split("?")[0].rstrip("/")
        if path == "/api/auth/login":
            if self._handle_auth_login():
                return
        if path == "/api/auth/logout":
            if self._handle_auth_logout():
                return
        if not self._require_auth_for_api():
            return
        if path == "/api/maintenance/phase1":
            if self._handle_maintenance_phase1():
                return
        if path == "/api/maintenance/phase2_1":
            if self._handle_maintenance_phase2_1():
                return
        if path == "/api/maintenance/phase2_2":
            if self._handle_maintenance_phase2_2():
                return
        if path == "/api/maintenance/mapping_update":
            if self._handle_maintenance_mapping_update():
                return
        if path == "/api/maintenance/newproducts_update":
            if self._handle_maintenance_newproducts_update():
                return
        if path == "/api/maintenance/add_to_product_mapping":
            if self._handle_maintenance_add_to_product_mapping():
                return
        if READ_ONLY_SERVER:
            self.send_error(405, "Method Not Allowed (read-only server)")
            self.log_message("BLOCKED POST %s", self.path)
        else:
            self.send_error(404, "Not Found")

    def do_DELETE(self):
        if READ_ONLY_SERVER:
            self.send_error(405, "Method Not Allowed (read-only server)")
            self.log_message("BLOCKED DELETE %s", self.path)
        else:
            self.send_error(404, "Not Found")

    def do_PATCH(self):
        if READ_ONLY_SERVER:
            self.send_error(405, "Method Not Allowed (read-only server)")
            self.log_message("BLOCKED PATCH %s", self.path)
        else:
            self.send_error(404, "Not Found")

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
