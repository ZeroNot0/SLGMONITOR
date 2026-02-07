# -*- coding: utf-8 -*-
"""MySQL 连接配置，从环境变量读取。"""
import os
from pathlib import Path

# 项目根目录（backend 的上级）
BASE_DIR = Path(__file__).resolve().parent.parent.parent

def get_mysql_config():
    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "slg_monitor"),
        "password": os.environ.get("MYSQL_PASSWORD", ""),
        "database": os.environ.get("MYSQL_DATABASE", "slg_monitor"),
        "charset": "utf8mb4",
    }

def use_mysql():
    """是否启用从 MySQL 读数据（环境变量 USE_MYSQL=1 时启用）。"""
    return os.environ.get("USE_MYSQL", "").strip() in ("1", "true", "yes")
