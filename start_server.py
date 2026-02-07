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
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

# 产品维度「爆量产品地区数据」空数据时的表头，与 frontend/convert_final_join_to_json.py 的 PRODUCT_DIMENSION_COLUMNS 一致
PRODUCT_STRATEGY_EMPTY_HEADERS = [
    "产品归属", "Unified ID", "公司归属", "第三方记录最早上线时间",
    "当周周安装", "上周周安装", "周安装变动",
    "亚洲 T1 市场获量", "欧美 T1 市场获量", "T2 市场获量", "T3 市场获量",
]


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
    """加载 deploy/auth_users.json，格式：{"users": [{"username", "salt", "hash", "role?", "status?"}]}。"""
    if not AUTH_USERS_PATH.is_file():
        return []
    try:
        data = json.loads(AUTH_USERS_PATH.read_text(encoding="utf-8"))
        return data.get("users") or []
    except Exception:
        return []


def _save_auth_users(users: list) -> bool:
    """写回 auth_users.json。"""
    try:
        AUTH_USERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        AUTH_USERS_PATH.write_text(
            json.dumps({"users": users}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return True
    except Exception:
        return False


def _get_user_by_username(username: str):
    """按用户名取用户记录，无则返回 None。"""
    for u in _load_auth_users():
        if (u.get("username") or "").strip() == (username or "").strip():
            return u
    return None


def _verify_password(username: str, password: str) -> tuple:
    """校验用户名与密码。返回 (ok: bool, role: str)。仅 status=approved 或 role=super_admin 允许登录。"""
    users = _load_auth_users()
    for u in users:
        if (u.get("username") or "").strip() != username.strip():
            continue
        salt = (u.get("salt") or "").encode("utf-8")
        h = (u.get("hash") or "").strip()
        if not h:
            return False, ""
        computed = hashlib.sha256(salt + password.encode("utf-8")).hexdigest()
        if not secrets.compare_digest(computed, h):
            return False, ""
        role = (u.get("role") or "user").strip() or "user"
        status = (u.get("status") or "approved").strip() or "approved"
        if role == "super_admin" or status == "approved":
            return True, role
        return False, ""  # 待审批不允许登录
    return False, ""


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

    def _handle_basetable_metrics_total_product_names_all(self):
        """GET /api/basetable/metrics_total_product_names_all：一次返回所有周的产品名与 Unified ID，供上线新游匹配，减少请求数。"""
        raw = self.path or ""
        if raw.split("?")[0].rstrip("/") != "/api/basetable/metrics_total_product_names_all":
            return False
        weeks_list = []
        if not WEEKS_INDEX_PATH.is_file():
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"weeks": []}, ensure_ascii=False).encode("utf-8"))
            return True
        try:
            index_data = json.loads(WEEKS_INDEX_PATH.read_text(encoding="utf-8"))
        except Exception:
            index_data = {}
        for year_s in (index_data or {}).keys():
            if not (year_s and str(year_s).isdigit() and len(str(year_s)) == 4):
                continue
            for week_tag in (index_data.get(year_s) or []):
                if not week_tag or not isinstance(week_tag, str):
                    continue
                json_path = FRONTEND_DIR / "data" / str(year_s) / week_tag / "metrics_total.json"
                if not json_path.is_file():
                    continue
                try:
                    data = json.loads(json_path.read_text(encoding="utf-8"))
                except Exception:
                    continue
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
                weeks_list.append({
                    "year": year_s,
                    "week": week_tag,
                    "productNames": product_names,
                    "nameToUnifiedId": name_to_id,
                })
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"weeks": weeks_list}, ensure_ascii=False).encode("utf-8"))
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

    def _handle_advanced_query(self):
        """GET /api/advanced_query/tables：表列表。GET /api/advanced_query/table/<name>：表结构+部分数据。仅超级管理员；需 MySQL。"""
        raw = (self.path or "").split("?")[0].rstrip("/")
        if not raw.startswith("/api/advanced_query"):
            return False
        if not self._require_super_admin():
            return True
        use_db = False
        conn = None
        try:
            from backend.db.config import use_mysql
            from backend.db.connection import get_connection
            from backend.db import advanced_query as aq
            use_db = use_mysql()
            conn = get_connection() if use_db else None
        except ImportError:
            pass
        if not conn:
            self.send_response(503)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": "高级查询需启用 MySQL"}, ensure_ascii=False).encode("utf-8"))
            return True
        try:
            from backend.db import advanced_query as aq
            if raw == "/api/advanced_query/tables":
                tables = aq.get_tables(conn)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"tables": tables}, ensure_ascii=False).encode("utf-8"))
                return True
            if raw.startswith("/api/advanced_query/table/"):
                name = raw[len("/api/advanced_query/table/"):].strip()
                if not name:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "message": "缺少表名"}, ensure_ascii=False).encode("utf-8"))
                    return True
                info = aq.get_table_info(conn, name)
                if info is None:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "message": "表不存在或无法访问"}, ensure_ascii=False).encode("utf-8"))
                    return True
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(info, ensure_ascii=False).encode("utf-8"))
                return True
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            return True
        self.send_response(404)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": False, "message": "Not Found"}, ensure_ascii=False).encode("utf-8"))
        return True

    def _handle_advanced_query_execute(self):
        """POST /api/advanced_query/execute：Body JSON { "sql": "..." }，执行 SQL 并返回结果或影响行数。仅超级管理员；需 MySQL。"""
        if not self._require_super_admin():
            return True
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length <= 0 or length > 1024 * 1024:
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": "请提供 SQL（Body 不超过 1MB）"}, ensure_ascii=False).encode("utf-8"))
            return True
        try:
            body = self.rfile.read(length).decode("utf-8", errors="replace")
            data = json.loads(body) if body.strip() else {}
        except Exception as e:
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": "JSON 解析失败: " + str(e)}, ensure_ascii=False).encode("utf-8"))
            return True
        sql = (data.get("sql") or "").strip()
        if not sql:
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": "SQL 不能为空"}, ensure_ascii=False).encode("utf-8"))
            return True
        conn = None
        try:
            from backend.db.config import use_mysql
            from backend.db.connection import get_connection
            from backend.db import advanced_query as aq
            if not use_mysql():
                self.send_response(503)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "高级查询需启用 MySQL"}, ensure_ascii=False).encode("utf-8"))
                return True
            conn = get_connection()
            if not conn:
                self.send_response(503)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "数据库连接失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            out = aq.execute_sql(conn, sql)
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            return True
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        if "error" in out:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": out["error"]}, ensure_ascii=False).encode("utf-8"))
            return True
        if "headers" in out:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "headers": out["headers"], "rows": out["rows"]}, ensure_ascii=False).encode("utf-8"))
            return True
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True, "affected": out.get("affected", 0)}, ensure_ascii=False).encode("utf-8"))
        return True

    def _handle_api_data(self):
        """GET /api/data/*：从 MySQL 读数据并返回 JSON；未启用 MySQL 或失败时返回 False 走原有逻辑。"""
        raw = (self.path or "").split("?")[0].rstrip("/")
        qs = (self.path or "").split("?", 1)[-1] if "?" in self.path else ""
        params = urllib.parse.parse_qs(qs)
        use_db = False
        try:
            from backend.db import api_data
            from backend.db.config import use_mysql
            use_db = use_mysql()
        except ImportError:
            pass

        def send_json(obj):
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "private, max-age=60")  # 取数接口缓存 1 分钟，减轻重复请求
            self.end_headers()
            self.wfile.write(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
            return True

        def read_json_path(path):
            if path and path.is_file():
                try:
                    return json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    pass
            return None

        if raw == "/api/data/weeks_index":
            out = api_data.get_weeks_index() if use_db else read_json_path(WEEKS_INDEX_PATH)
            if out is not None:
                return send_json(out)
            if use_db:
                return send_json({})
            return False
        if raw == "/api/data/formatted":
            year = (params.get("year") or [""])[0].strip()
            week = (params.get("week") or [""])[0].strip()
            if not year or not week:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "year and week required"}, ensure_ascii=False).encode("utf-8"))
                return True
            out = api_data.get_formatted(year, week) if use_db else read_json_path(FRONTEND_DIR / "data" / year / (week + "_formatted.json"))
            if out is not None:
                return send_json(out)
            # 启用 MySQL 但该周无数据时仍返回 JSON，避免请求落到静态文件导致 404 File not found
            if use_db:
                return send_json({"headers": [], "rows": [], "styles": []})
            return False
        if raw == "/api/data/product_strategy":
            year = (params.get("year") or [""])[0].strip()
            week = (params.get("week") or [""])[0].strip()
            typ = (params.get("type") or ["old"])[0].strip().lower()
            if typ not in ("old", "new"):
                typ = "old"
            if not year or not week:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "year and week required"}, ensure_ascii=False).encode("utf-8"))
                return True
            fn = "product_strategy_old.json" if typ == "old" else "product_strategy_new.json"
            out = api_data.get_product_strategy(year, week, typ) if use_db else read_json_path(FRONTEND_DIR / "data" / year / week / fn)
            if out is not None:
                return send_json(out)
            # 无数据时仍返回标准表头，前端显示表头+“无数据”而非整页空白
            return send_json({"headers": PRODUCT_STRATEGY_EMPTY_HEADERS, "rows": []})
        if raw == "/api/data/product_detail_panels":
            year = (params.get("year") or [""])[0].strip()
            week = (params.get("week") or [""])[0].strip()
            unified_id = (params.get("unified_id") or [""])[0].strip() or None
            product_name = (params.get("product_name") or [""])[0].strip() or None
            if not year or not week or (not unified_id and not product_name):
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "year, week, and unified_id or product_name required"}, ensure_ascii=False).encode("utf-8"))
                return True
            if use_db:
                out = api_data.get_product_detail_panels(year, week, unified_id=unified_id, product_name=product_name)
                if out is not None:
                    return send_json(out)
            self.send_response(404)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "no data for this product in this week"}, ensure_ascii=False).encode("utf-8"))
            return True
        if raw == "/api/data/company_detail_panels":
            year = (params.get("year") or [""])[0].strip()
            week = (params.get("week") or [""])[0].strip()
            company = (params.get("company") or [""])[0].strip() or None
            if not year or not week or not company:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "year, week and company required"}, ensure_ascii=False).encode("utf-8"))
                return True
            if use_db:
                out = api_data.get_company_detail_panels(year, week, company)
                if out is not None:
                    return send_json(out)
            self.send_response(404)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "no data for this company in this week"}, ensure_ascii=False).encode("utf-8"))
            return True
        if raw == "/api/data/creative_products":
            year = (params.get("year") or [""])[0].strip()
            week = (params.get("week") or [""])[0].strip()
            if not year or not week:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "year and week required"}, ensure_ascii=False).encode("utf-8"))
                return True
            out = api_data.get_creative_products(year, week) if use_db else read_json_path(FRONTEND_DIR / "data" / year / week / "creative_products.json")
            if out is not None:
                return send_json(out)
            if use_db:
                return send_json({})
            return False
        if raw == "/api/data/metrics_total":
            year = (params.get("year") or [""])[0].strip()
            week = (params.get("week") or [""])[0].strip()
            if not year or not week:
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
            if use_db:
                out = api_data.get_metrics_total(year, week, limit=limit, q=q)
            else:
                data = read_json_path(FRONTEND_DIR / "data" / year / week / "metrics_total.json")
                if not data:
                    out = None
                else:
                    headers = data.get("headers") or []
                    rows = data.get("rows") or []
                    if q:
                        q_lower = q.lower()
                        rows = [r for r in rows if any(str(c or "").lower().find(q_lower) >= 0 for c in (r or []))]
                    out = {"headers": headers, "rows": rows[:limit], "total": len(rows)}
            if out is not None:
                return send_json(out)
            if use_db:
                return send_json({"headers": [], "rows": [], "total": 0})
            return False
        if raw == "/api/data/metrics_total_product_names":
            year = (params.get("year") or [""])[0].strip()
            week = (params.get("week") or [""])[0].strip()
            if not year or not week:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "year and week required"}, ensure_ascii=False).encode("utf-8"))
                return True
            out = api_data.get_metrics_total_product_names(year, week) if use_db else None
            if not use_db:
                data = read_json_path(FRONTEND_DIR / "data" / year / week / "metrics_total.json")
                if data:
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
                        name_val = (str(row[name_idx]).strip() if name_idx < len(row) and row[name_idx] is not None else "")
                        if not name_val:
                            continue
                        if name_val not in seen:
                            seen.add(name_val)
                            product_names.append(name_val)
                        if id_idx >= 0 and name_val and name_val not in name_to_id:
                            id_val = (str(row[id_idx]).strip() if id_idx < len(row) and row[id_idx] is not None else "")
                            if id_val:
                                name_to_id[name_val] = id_val
                    out = {"productNames": product_names, "nameToUnifiedId": name_to_id}
            if out is not None:
                return send_json(out)
            if use_db:
                return send_json({"productNames": [], "nameToUnifiedId": {}})
            return False
        if raw == "/api/data/metrics_total_product_names_all":
            out = api_data.get_metrics_total_product_names_all() if use_db else None
            if not use_db and WEEKS_INDEX_PATH.is_file():
                wi = read_json_path(WEEKS_INDEX_PATH)
                if wi:
                    weeks_list = []
                    for ys, wl in wi.items():
                        if ys == "data_range" or not isinstance(wl, list):
                            continue
                        for wt in wl:
                            if not isinstance(wt, str):
                                continue
                            data = read_json_path(FRONTEND_DIR / "data" / ys / wt / "metrics_total.json")
                            if not data:
                                continue
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
                                name_val = (str(row[name_idx]).strip() if name_idx < len(row) and row[name_idx] is not None else "")
                                if not name_val:
                                    continue
                                if name_val not in seen:
                                    seen.add(name_val)
                                    product_names.append(name_val)
                                if id_idx >= 0 and name_val and name_val not in name_to_id:
                                    id_val = (str(row[id_idx]).strip() if id_idx < len(row) and row[id_idx] is not None else "")
                                    if id_val:
                                        name_to_id[name_val] = id_val
                            weeks_list.append({"year": ys, "week": wt, "productNames": product_names, "nameToUnifiedId": name_to_id})
                    out = {"weeks": weeks_list}
            if out is not None:
                return send_json(out)
            if use_db:
                return send_json({"weeks": []})
            return False
        if raw == "/api/data/new_products":
            out = api_data.get_new_products() if use_db else read_json_path(FRONTEND_DIR / "data" / "new_products.json")
            if out is not None:
                return send_json(out)
            if use_db:
                return send_json({"headers": [], "rows": []})
            return False
        if raw == "/api/data/product_theme_style_mapping":
            out = api_data.get_product_theme_style_mapping() if use_db else read_json_path(THEME_STYLE_MAPPING_PATH)
            if out is not None:
                return send_json(out)
            if use_db:
                return send_json({"byUnifiedId": {}, "byProductName": {}})
            return False
        if raw == "/api/basetable" and params.get("name"):
            name = (params.get("name") or [""])[0].strip()
            if name in BASETABLE_SOURCES:
                out = api_data.get_basetable(name) if use_db else None
                if not use_db:
                    headers, rows = _excel_to_headers_rows(BASETABLE_SOURCES[name])
                    out = {"headers": headers, "rows": rows} if (headers or rows) else None
                if out is not None:
                    return send_json(out)
        return False

    def _get_cookie(self, name: str):
        """从请求头 Cookie 中解析指定名称的值。"""
        cookie = self.headers.get("Cookie") or ""
        for part in cookie.split(";"):
            part = part.strip()
            if part.startswith(name + "="):
                return urllib.parse.unquote(part[len(name) + 1 :].strip())
        return None

    def _get_session_info(self):
        """从 Cookie 读取 session，返回 {"username", "role"} 或 None。"""
        sid = self._get_cookie(AUTH_COOKIE_NAME)
        if not sid:
            return None
        with AUTH_SESSIONS_LOCK:
            info = AUTH_SESSIONS.get(sid)
        if not info:
            return None
        return {"username": info.get("username"), "role": (info.get("role") or "user")}

    def _get_session_username(self):
        info = self._get_session_info()
        return (info.get("username") if info else None) or None

    def _require_auth_for_api(self):
        """对需要登录的 /api/* 校验 session；公开接口返回 True。若未登录则发送 401 并返回 False。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path in ("/api/auth/check", "/api/auth/login", "/api/auth/register"):
            return True
        if path.startswith("/api/data/"):
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

    def _require_super_admin(self):
        """要求当前用户为 super_admin，否则 403。在 _require_auth_for_api 通过后调用。"""
        info = self._get_session_info()
        if not info or info.get("role") != "super_admin":
            self.send_response(403)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": "需要超级管理员权限"}, ensure_ascii=False).encode("utf-8"))
            return False
        return True

    def _handle_auth_check(self):
        """GET /api/auth/check：校验当前 Cookie 对应 session，返回 {ok, username, role} 或 401。"""
        raw = (self.path or "").split("?")[0].rstrip("/")
        if raw != "/api/auth/check":
            return False
        info = self._get_session_info()
        if not info or not info.get("username"):
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
        self.wfile.write(json.dumps({
            "ok": True,
            "username": info.get("username"),
            "role": info.get("role") or "user"
        }, ensure_ascii=False).encode("utf-8"))
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
            ok, role = _verify_password(username, password)
            if not ok:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "用户名或密码错误，或账号尚未通过审批"}, ensure_ascii=False).encode("utf-8"))
                return True
            session_id = secrets.token_urlsafe(32)
            with AUTH_SESSIONS_LOCK:
                AUTH_SESSIONS[session_id] = {"username": username, "role": role or "user"}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header(
                "Set-Cookie",
                "%s=%s; Path=/; Max-Age=%d; HttpOnly; SameSite=Lax"
                % (AUTH_COOKIE_NAME, session_id, AUTH_COOKIE_MAX_AGE),
            )
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "username": username, "role": role or "user"}, ensure_ascii=False).encode("utf-8"))
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

    def _handle_auth_register(self):
        """POST /api/auth/register：Body JSON {username, password}，注册为普通用户，status=pending，需审批后登录。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path != "/api/auth/register":
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
            if len(username) < 2:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "用户名至少 2 个字符"}, ensure_ascii=False).encode("utf-8"))
                return True
            if not password or len(password) < 6:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "密码至少 6 位"}, ensure_ascii=False).encode("utf-8"))
                return True
            if _get_user_by_username(username):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "该用户名已被注册"}, ensure_ascii=False).encode("utf-8"))
                return True
            salt = secrets.token_hex(16)
            h = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
            users = _load_auth_users()
            users.append({
                "username": username,
                "salt": salt,
                "hash": h,
                "role": "user",
                "status": "pending",
            })
            if not _save_auth_users(users):
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "写入失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "message": "注册成功，请等待管理员审批通过后登录"}, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("auth/register error: %s", e)
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            return True

    def _handle_auth_approved_users(self):
        """GET /api/auth/approved_users：超级管理员可见，返回已审批用户列表（status=approved 或 super_admin）。数据来自 deploy/auth_users.json，无 MySQL 用户表。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path != "/api/auth/approved_users":
            return False
        if not self._require_super_admin():
            return True
        try:
            users = _load_auth_users()
            approved = []
            for u in users:
                status = str(u.get("status") or "approved").strip() or "approved"
                role = str(u.get("role") or "user").strip() or "user"
                if status == "approved" or role == "super_admin":
                    approved.append({
                        "username": u.get("username"),
                        "role": role,
                        "status": status,
                    })
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "users": approved}, ensure_ascii=False).encode("utf-8"))
        except Exception as e:
            self.log_message("auth/approved_users error: %s", e)
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": str(e), "users": []}, ensure_ascii=False).encode("utf-8"))
        return True

    def _handle_auth_pending_users(self):
        """GET /api/auth/pending_users：超级管理员可见，返回待审批用户列表。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path != "/api/auth/pending_users":
            return False
        if not self._require_super_admin():
            return True
        users = _load_auth_users()
        pending = [{"username": u.get("username")} for u in users if (u.get("status") or "").strip() == "pending"]
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True, "users": pending}, ensure_ascii=False).encode("utf-8"))
        return True

    def _handle_auth_approve(self):
        """POST /api/auth/approve：超级管理员审批，Body JSON {username}，将用户 status 设为 approved。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path != "/api/auth/approve":
            return False
        if not self._require_super_admin():
            return True
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
            if not username:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "请指定用户名"}, ensure_ascii=False).encode("utf-8"))
                return True
            users = _load_auth_users()
            found = False
            for u in users:
                if (u.get("username") or "").strip() == username:
                    u["status"] = "approved"
                    found = True
                    break
            if not found:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "用户不存在"}, ensure_ascii=False).encode("utf-8"))
                return True
            if not _save_auth_users(users):
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "写入失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "message": "已审批通过"}, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("auth/approve error: %s", e)
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            return True

    def do_GET(self):
        self._send_no_content = False
        self._allow_cache = False  # 默认不缓存，接口与 HTML 保持实时
        self._fix_typo_path()
        if getattr(self, "_send_no_content", False):
            self.send_response(204)
            self.end_headers()
            return
        if self._handle_auth_check():
            return
        if not self._require_auth_for_api():
            return
        if self._handle_auth_approved_users():
            return
        if self._handle_auth_pending_users():
            return
        if self._handle_video_proxy():
            return
        if self._handle_maintenance_download():
            return
        if self._handle_api_data():
            return
        if self._handle_basetable_metrics_total():
            return
        if self._handle_basetable_metrics_total_product_names_all():
            return
        if self._handle_basetable_metrics_total_product_names():
            return
        if self._handle_basetable():
            return
        if self._handle_advanced_query():
            return
        if self._serve_frontend_index_with_weeks():
            return
        # 静态资源：JS/CSS/前端 data 下 JSON 允许短时缓存，减轻重复请求
        path = (self.path or "").split("?")[0]
        if path.startswith("/frontend/js/") or path.startswith("/frontend/css/"):
            self._allow_cache = True
        elif path.startswith("/frontend/data/") and path.endswith(".json"):
            self._allow_cache = True
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
        if getattr(self, "_allow_cache", False):
            self.send_header("Cache-Control", "public, max-age=300")  # 静态资源缓存 5 分钟
        else:
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        super().end_headers()

    def do_OPTIONS(self):
        """CORS 预检：允许对 maintenance、auth 接口的 POST，避免浏览器报 Method Not Allowed。"""
        path = (self.path or "").split("?")[0].rstrip("/")
        if path in ("/api/maintenance/phase1", "/api/maintenance/phase1_table_only", "/api/maintenance/refresh_weeks_index", "/api/maintenance/phase2_1", "/api/maintenance/phase2_2", "/api/maintenance/mapping_update", "/api/maintenance/newproducts_update", "/api/maintenance/add_to_product_mapping", "/api/auth/login", "/api/auth/logout", "/api/auth/register", "/api/auth/approve", "/api/advanced_query/execute"):
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
            # 第一步完成后若启用 MySQL：将本周文件（含 product_strategy 爆量产品）同步到库，前端/接口才能看到更新
            synced_mysql = False
            try:
                from backend.db.config import use_mysql
                from backend.db.connection import get_connection
                from backend.db.sync_week import sync_week_from_files, refresh_weeks_index
            except ImportError:
                pass
            else:
                if use_mysql():
                    conn = get_connection()
                    if conn:
                        try:
                            sync_week_from_files(conn, year, week_val, BASE_DIR)
                            refresh_weeks_index(conn, year, week_val)
                            synced_mysql = True
                        finally:
                            conn.close()
            if synced_mysql:
                try:
                    from backend.db import api_data
                    api_data.invalidate_weeks_index()
                except Exception:
                    pass
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
            msg = "第一步执行完成，公司维度大盘数据已更新；metrics_total 已转为 JSON，数据底表「产品总表」可查看该周。"
            if synced_mysql:
                msg += " 爆量产品（product_strategy）已同步写入 MySQL，产品维度可查看该周。"
            self.wfile.write(json.dumps({
                "ok": True,
                "message": msg,
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

    def _handle_maintenance_refresh_weeks_index(self):
        """POST /api/maintenance/refresh_weeks_index：仅将 (year, week_tag) 加入周索引。Body JSON: { year, week_tag }。数据已写入 MySQL 时使用，无需上传 CSV。"""
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
            year_val = str(data.get("year") or "").strip()
            week_val = str(data.get("week_tag") or "").strip()
            if not year_val.isdigit() or len(year_val) != 4:
                self.send_error(400, "year must be 4 digits")
                return True
            if not re.match(r"^\d{4}-\d{4}$", week_val):
                self.send_error(400, "week_tag must be like 0119-0125")
                return True
            year = int(year_val)
            try:
                from backend.db.config import use_mysql
                from backend.db.connection import get_connection
                from backend.db.sync_week import refresh_weeks_index
            except ImportError:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "未启用 MySQL，无法刷新周索引"}, ensure_ascii=False).encode("utf-8"))
                return True
            if not use_mysql():
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "未启用 MySQL（USE_MYSQL=1 时可用）"}, ensure_ascii=False).encode("utf-8"))
                return True
            conn = get_connection()
            if not conn:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "无法连接 MySQL"}, ensure_ascii=False).encode("utf-8"))
                return True
            try:
                ok = refresh_weeks_index(conn, year, week_val)
            finally:
                conn.close()
            if ok:
                try:
                    from backend.db import api_data
                    api_data.invalidate_weeks_index()
                except Exception:
                    pass
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": ok,
                "message": "周索引已刷新，该周已加入可选列表。" if ok else "刷新周索引失败。"
            }, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("maintenance/refresh_weeks_index error: %s", e)
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False).encode("utf-8"))
            except Exception:
                pass
            return True

    def _handle_maintenance_phase1_table_only(self):
        """POST /api/maintenance/phase1_table_only：从已有数据完成第一步（制表 + 刷新周索引）。Body JSON: { year, week_tag }。不传 CSV。若有 output/intermediate 文件则执行 step5 并同步到 MySQL；已启用 MySQL 时始终刷新周索引。"""
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
            year_val = str(data.get("year") or "").strip()
            week_val = str(data.get("week_tag") or "").strip()
            if not year_val.isdigit() or len(year_val) != 4:
                self.send_error(400, "year must be 4 digits")
                return True
            if not re.match(r"^\d{4}-\d{4}$", week_val):
                self.send_error(400, "week_tag must be like 0119-0125")
                return True
            year = int(year_val)
            from run_full_pipeline import run_phase3
            step5_ok = run_phase3(week_val, year)
            refreshed_index = False
            try:
                from backend.db.config import use_mysql
                from backend.db.connection import get_connection
                from backend.db.sync_week import sync_week_from_files, refresh_weeks_index
            except ImportError:
                pass
            else:
                if use_mysql():
                    conn = get_connection()
                    if conn:
                        try:
                            sync_week_from_files(conn, year, week_val, BASE_DIR)
                            refresh_weeks_index(conn, year, week_val)
                            refreshed_index = True
                        finally:
                            conn.close()
            if refreshed_index:
                try:
                    from backend.db import api_data
                    api_data.invalidate_weeks_index()
                except Exception:
                    pass
            if step5_ok:
                msg = "第一步完成：已制表并同步到 MySQL，周索引已刷新。" if refreshed_index else "第一步完成：已制表；未启用 MySQL 时请刷新页面查看。"
            else:
                msg = "该周无产出文件，已仅刷新周索引；若数据已写入 MySQL 可直接选周查看。" if refreshed_index else "该周无产出文件且未启用 MySQL，请先上传 CSV 执行完整第一步或启用 MySQL 后写入数据。"
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "message": msg
            }, ensure_ascii=False).encode("utf-8"))
            return True
        except Exception as e:
            self.log_message("maintenance/phase1_table_only error: %s", e)
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
        self.log_message("maintenance/phase2_1 开始执行（将调用 ST 地区数据 API）")
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
            unified_id = (data.get("unified_id") or "").strip() or None
            from run_full_pipeline import run_phase2, run_phase3, classify_single_product_to_target
            if not run_phase2(
                week_val, year,
                fetch_country=True,
                fetch_creatives=False,
                limit=limit,
                target_source=target,
                product_type=product_type,
                unified_id=unified_id,
            ):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "2.1 步拉取地区数据执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            if unified_id:
                classify_single_product_to_target(year, week_val, unified_id)
            if not run_phase3(week_val, year):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "前端更新执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            synced_mysql = False
            try:
                from backend.db.config import use_mysql
                from backend.db.connection import get_connection
                from backend.db.sync_week import sync_week_from_files
            except ImportError:
                pass
            else:
                if use_mysql():
                    conn = get_connection()
                    if conn:
                        try:
                            sync_week_from_files(conn, year, week_val, BASE_DIR)
                            synced_mysql = True
                        finally:
                            conn.close()
            msg = "2.1 步执行完成，目标产品分地区数据已拉取并已更新前端。"
            if synced_mysql:
                msg += " 已同步写入 MySQL（product_strategy、creative_products 等）。"
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "message": msg
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
        self.log_message("maintenance/phase2_2 开始执行（将调用 ST 创意数据 API）")
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
            unified_id = (data.get("unified_id") or "").strip() or None
            from run_full_pipeline import run_phase2, run_phase3, classify_single_product_to_target
            if not run_phase2(
                week_val, year,
                fetch_country=False,
                fetch_creatives=True,
                limit=limit,
                target_source=target,
                product_type=product_type,
                unified_id=unified_id,
            ):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "2.2 步拉取创意数据执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            if unified_id:
                classify_single_product_to_target(year, week_val, unified_id)
            if not run_phase3(week_val, year):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "message": "前端更新执行失败"}, ensure_ascii=False).encode("utf-8"))
                return True
            synced_mysql = False
            try:
                from backend.db.config import use_mysql
                from backend.db.connection import get_connection
                from backend.db.sync_week import sync_week_from_files
            except ImportError:
                pass
            else:
                if use_mysql():
                    conn = get_connection()
                    if conn:
                        try:
                            sync_week_from_files(conn, year, week_val, BASE_DIR)
                            synced_mysql = True
                        finally:
                            conn.close()
            msg = "2.2 步执行完成，目标产品创意数据已拉取并已更新前端。"
            if synced_mysql:
                msg += " 已同步写入 MySQL（product_strategy、creative_products 等）。"
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "message": msg
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
                    try:
                        from backend.db.config import use_mysql
                        from backend.db.connection import get_connection
                        from backend.db.sync_maintenance import sync_basetable_from_files
                    except ImportError:
                        pass
                    else:
                        if use_mysql():
                            conn = get_connection()
                            if conn:
                                try:
                                    if sync_basetable_from_files(conn, BASE_DIR):
                                        msg = msg + " 已同步到 MySQL。"
                                finally:
                                    conn.close()
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
                    try:
                        from backend.db.config import use_mysql
                        from backend.db.connection import get_connection
                        from backend.db.sync_maintenance import sync_new_products_from_file
                    except ImportError:
                        pass
                    else:
                        if use_mysql():
                            conn = get_connection()
                            if conn:
                                try:
                                    if sync_new_products_from_file(conn, BASE_DIR):
                                        msg = (msg if msg else "新产品监测表已更新。") + " 已同步到 MySQL。"
                                finally:
                                    conn.close()
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
        """POST /api/maintenance/add_to_product_mapping：仅超级管理员可调。Body JSON { products: [ { 产品名, 产品归属, Unified ID, 题材, 画风, 发行商, 公司归属 } ] }，写入 MySQL 或 Excel（仅追加 产品归属 不在表中的行），写入时包含总表对应的 Unified ID。"""
        try:
            if not self._require_super_admin():
                return True
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
            OUT_COLS = ["产品名（实时更新中）", "Unified ID", "产品归属", "题材", "画风", "发行商", "公司归属"]
            normalized = []
            for p in products:
                if not isinstance(p, dict):
                    continue
                prod_name = (p.get("产品名") or p.get("产品名（实时更新中）") or "").strip()
                prod_belong = (p.get("产品归属") or p.get("产品名") or p.get("产品名（实时更新中）") or "").strip()
                if not prod_belong:
                    continue
                theme = (p.get("题材") or "").strip()
                style = (p.get("画风") or "").strip()
                pub = (p.get("发行商") or "").strip()
                comp = (p.get("公司归属") or "").strip()
                if not comp:
                    comp = "未知"
                unified_id = (p.get("Unified ID") or p.get("unifiedId") or "").strip()
                normalized.append([
                    prod_name or prod_belong,
                    unified_id,
                    prod_belong,
                    theme,
                    style,
                    pub,
                    comp,
                ])
            if not normalized:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "message": "无有效产品数据", "added": 0}, ensure_ascii=False).encode("utf-8"))
                return True
            use_mysql = False
            try:
                from backend.db.config import use_mysql as _use_mysql
                from backend.db.connection import get_connection
                from backend.db.sync_maintenance import append_product_mapping_rows
                use_mysql = _use_mysql
            except ImportError:
                pass
            if use_mysql and use_mysql():
                conn = get_connection()
                if conn:
                    try:
                        added = append_product_mapping_rows(conn, normalized)
                    finally:
                        conn.close()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json; charset=utf-8")
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.end_headers()
                        if added > 0:
                            self.wfile.write(json.dumps({"ok": True, "message": "已成功将 %d 条新产品加入产品归属表（已写入 MySQL，含 Unified ID）" % added, "added": added}, ensure_ascii=False).encode("utf-8"))
                        else:
                            self.wfile.write(json.dumps({"ok": True, "message": "所选产品均已在产品归属表中，无新增", "added": 0}, ensure_ascii=False).encode("utf-8"))
                        return True
            import pandas as pd
            PROD_XLSX = MAPPING_DIR / "产品归属.xlsx"
            COMP_XLSX = MAPPING_DIR / "公司归属.xlsx"
            df_new = pd.DataFrame(normalized, columns=OUT_COLS)
            existing_belong = set()
            df_old = pd.DataFrame()
            if PROD_XLSX.exists():
                try:
                    df_old = pd.read_excel(PROD_XLSX)
                    if not df_old.empty and "产品归属" in df_old.columns:
                        existing_belong = set(df_old["产品归属"].astype(str).str.strip().replace("nan", "").dropna().unique())
                    elif not df_old.empty and df_old.shape[1] >= 5:
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
        if path == "/api/auth/register":
            if self._handle_auth_register():
                return
        if not self._require_auth_for_api():
            return
        if path == "/api/auth/approve":
            if self._handle_auth_approve():
                return
        if path == "/api/maintenance/phase1":
            if self._handle_maintenance_phase1():
                return
        if path == "/api/maintenance/refresh_weeks_index":
            if self._handle_maintenance_refresh_weeks_index():
                return
        if path == "/api/maintenance/phase1_table_only":
            if self._handle_maintenance_phase1_table_only():
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
        if path == "/api/advanced_query/execute":
            if self._handle_advanced_query_execute():
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


def _ensure_mysql_and_check_tables():
    """当 USE_MYSQL=1 时：若连不上 MySQL 则尝试自动启动服务；连上后检查是否有表，无表则提示导入 schema。"""
    try:
        from backend.db.config import use_mysql
        if not use_mysql():
            return
    except ImportError:
        return
    conn = None
    try:
        from backend.db.connection import get_connection
        conn = get_connection()
    except Exception:
        pass
    if conn is None:
        print("MySQL 未连接，尝试自动启动 MySQL 服务…", flush=True)
        try:
            if sys.platform == "darwin":
                subprocess.run(["brew", "services", "start", "mysql"], capture_output=True, timeout=10)
            elif sys.platform.startswith("linux"):
                # 常见服务名为 mysql 或 mysqld（如 OpenCloudOS/CentOS 为 mysqld）
                for svc in ("mysql", "mysqld", "mariadb"):
                    r = subprocess.run(["systemctl", "start", svc], capture_output=True, timeout=10)
                    if r.returncode == 0:
                        break
            else:
                subprocess.run(["net", "start", "mysql"], capture_output=True, timeout=10, shell=True)
        except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
            pass
        time.sleep(3)
        try:
            conn = get_connection()
        except Exception:
            pass
    if conn is None:
        print("  → MySQL 仍无法连接。请确认：1) MySQL 已安装并启动（Mac: brew services start mysql）；2) 环境变量 MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE 正确。", flush=True)
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()")
            row = cur.fetchone()
            n = (row.get("n") or 0) if isinstance(row, dict) else (row[0] if row else 0)
        if n == 0:
            schema_path = BASE_DIR / "backend" / "db" / "schema.sql"
            print("  → 当前数据库暂无表。请先导入表结构：", flush=True)
            print(f"     mysql -u root -p {os.environ.get('MYSQL_DATABASE', 'slg_monitor')} < {schema_path}", flush=True)
        else:
            print(f"  → MySQL 已连接，当前库有 {n} 张表。", flush=True)
    except Exception as e:
        print(f"  → 检查表数量时出错: {e}", flush=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
    if os.environ.get("USE_MYSQL", "").strip() in ("1", "true", "yes"):
        _ensure_mysql_and_check_tables()
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
