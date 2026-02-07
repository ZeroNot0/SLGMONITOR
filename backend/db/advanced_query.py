# -*- coding: utf-8 -*-
"""高级查询：表列表、表详情、执行 SQL。"""
from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Optional, Dict

MAX_SELECT_ROWS = 10000
SAMPLE_ROWS = 50


def _cell_to_json(v):
    """将 MySQL 返回的 datetime/decimal/bytes 等转为可 JSON 序列化的值。"""
    if v is None:
        return None
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    if isinstance(v, datetime.timedelta):
        return str(v)
    if isinstance(v, Decimal):
        return float(v) if v == v else str(v)  # NaN 用 str
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, (dict, list)):
        return v  # JSON 列可能已是 dict/list
    return v


def get_tables(conn) -> list:
    """返回当前库所有表名（来自 information_schema）。"""
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME"
            )
            rows = cur.fetchall()
            return [r["TABLE_NAME"] for r in rows if r.get("TABLE_NAME")]
    except Exception:
        return []


def get_table_info(conn, table_name: str) -> Optional[Dict]:
    """返回表结构（DESCRIBE）及前 SAMPLE_ROWS 行数据。表名仅允许字母数字下划线。"""
    if not conn or not table_name:
        return None
    safe_name = "".join(c for c in table_name if c.isalnum() or c == "_")
    if safe_name != table_name:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("DESCRIBE `%s`" % safe_name)
            cols = cur.fetchall()
            columns = [c.get("Field") or "" for c in cols]
            cur.execute("SELECT * FROM `%s` LIMIT %s" % (safe_name, SAMPLE_ROWS))
            rows = cur.fetchall()
            list_rows = []
            for r in rows:
                list_rows.append([_cell_to_json(r.get(k)) for k in columns])
            return {"headers": columns, "rows": list_rows}
    except Exception:
        return None


def execute_sql(conn, sql: str, max_rows: int = MAX_SELECT_ROWS) -> dict:
    """
    执行 SQL。SELECT 返回 { "headers": [...], "rows": [...] }，最多 max_rows 行；
    INSERT/UPDATE/DELETE 返回 { "affected": n }；
    出错返回 { "error": "message" }。
    """
    if not conn or not (sql or "").strip():
        return {"error": "SQL 为空"}
    sql = sql.strip()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            upper = sql.upper()
            if upper.startswith("SELECT"):
                rows = cur.fetchall()
                if not rows:
                    return {"headers": [], "rows": []}
                headers = list(rows[0].keys())
                list_rows = [[_cell_to_json(r.get(h)) for h in headers] for r in rows[:max_rows]]
                return {"headers": headers, "rows": list_rows}
            return {"affected": cur.rowcount}
    except Exception as e:
        return {"error": str(e)}
