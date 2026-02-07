# -*- coding: utf-8 -*-
"""MySQL 连接封装，供迁移脚本和 API 使用。"""
import json

def get_connection():
    """获取 pymysql 连接，失败返回 None。"""
    try:
        import pymysql
    except ImportError:
        return None
    from .config import get_mysql_config
    cfg = get_mysql_config()
    if not cfg.get("password") and not __name__.endswith("migrate_data"):
        pass  # 迁移脚本可单独传参
    try:
        return pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            charset=cfg["charset"],
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception:
        return None

def json_dumps(obj):
    """与前端一致的 JSON 序列化。"""
    return json.dumps(obj, ensure_ascii=False)
