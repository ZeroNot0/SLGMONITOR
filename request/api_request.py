#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API 请求公共模块：读取 token、维护 session、统一请求入口。
供 fetch_country_data、fetch_ad_creatives 等脚本调用。
"""

import os
import sys
from pathlib import Path

# 项目根目录（request 的上一级）
BASE_DIR = Path(__file__).resolve().parent.parent
REQUEST_DIR = Path(__file__).resolve().parent

try:
    from app.app_paths import get_data_root
    TOKEN_FILE = get_data_root() / "request" / "token.txt"
except Exception:
    TOKEN_FILE = REQUEST_DIR / "token.txt"

# API 基础地址（请根据实际 Sensor Tower / 内部 API 文档替换）
# 可从环境变量 OVERRIDE_API_BASE 覆盖
API_BASE = os.environ.get("OVERRIDE_API_BASE", "https://api.sensortower.com/v1")


def load_token(token_path=None):
    """从 request/token.txt 读取 API token，去掉首尾空白。"""
    path = Path(token_path or TOKEN_FILE)
    if not path.exists():
        fallback = REQUEST_DIR / "token.txt"
        if fallback.exists():
            path = fallback
        else:
            raise FileNotFoundError(f"未找到 token 文件: {path}，请创建并填入 API token")
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError(f"token 文件为空: {path}")
    return token


def get_session(token=None):
    """返回带认证头的 requests.Session。"""
    import requests
    token = token or load_token()
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return session


def get(session, path, params=None, timeout=90):
    """
    发起 GET 请求。path 为相对 API_BASE 的路径，如 "/downloads/country".
    返回 response 对象，调用方负责 response.raise_for_status() 和 response.json()。
    """
    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    # 调试：打印实际请求的 URL 和参数
    import os
    if os.environ.get("DEBUG_API") == "1":
        print(f"  [DEBUG] 请求 URL: {url}")
        print(f"  [DEBUG] 请求参数: {params}")
    return session.get(url, params=params, timeout=timeout)


def post(session, path, json=None, data=None, timeout=60):
    """发起 POST 请求。"""
    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    return session.post(url, json=json, data=data, timeout=timeout)


def ensure_dir(path):
    """确保目录存在。"""
    Path(path).mkdir(parents=True, exist_ok=True)
