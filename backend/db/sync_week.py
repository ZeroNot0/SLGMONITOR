# -*- coding: utf-8 -*-
"""
单周数据同步与周索引刷新。
- refresh_weeks_index: 仅将 (year, week_tag) 加入周索引（数据已写入 MySQL 时用）。
- sync_week_from_files: 从 frontend/data/{年}/{周}/ 读取 JSON，写入 MySQL 并更新周索引（仅制表后同步用）。
"""
import json
from pathlib import Path

try:
    import pymysql
except ImportError:
    pymysql = None

def _get_base_dir():
    from .config import BASE_DIR
    return BASE_DIR

def refresh_weeks_index(conn, year: int, week_tag: str) -> bool:
    """
    将 (year, week_tag) 加入 year_weeks 与 app_config.weeks_index，不读文件。
    数据已直接写入 formatted_data/metrics_total 时，调用此接口即可让前端选到该周。
    """
    if not conn or not pymysql:
        return False
    year = int(year)
    week_tag = (week_tag or "").strip()
    if not week_tag or len(week_tag) < 7:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT IGNORE INTO year_weeks (year, week_tag) VALUES (%s, %s)", (year, week_tag))
            cur.execute("SELECT config_value FROM app_config WHERE config_key = 'weeks_index'")
            row = cur.fetchone()
            data = {}
            if row and row.get("config_value"):
                try:
                    data = json.loads(row["config_value"])
                except Exception:
                    data = {}
            year_s = str(year)
            if year_s not in data or not isinstance(data[year_s], list):
                data[year_s] = []
            if week_tag not in data[year_s]:
                data[year_s].append(week_tag)
                data[year_s].sort()
            val = json.dumps(data, ensure_ascii=False)
            cur.execute(
                "INSERT INTO app_config (config_key, config_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)",
                ("weeks_index", val),
            )
        conn.commit()
        return True
    except Exception:
        if conn:
            conn.rollback()
        return False


def sync_week_from_files(conn, year: int, week_tag: str, base_dir: Path = None) -> bool:
    """
    从 frontend/data/{year}/{week_tag}/ 及同目录下 {week_tag}_formatted.json 读取，
    写入 formatted_data、metrics_total、product_strategy（old/new）、creative_products，
    并刷新周索引。2.1/2.2 步拉取完成后调用即可将新数据写入 MySQL。
    """
    if not conn or not pymysql:
        return False
    base_dir = base_dir or _get_base_dir()
    year = int(year)
    week_tag = (week_tag or "").strip()
    data_dir = base_dir / "frontend" / "data" / str(year)
    if not data_dir.is_dir():
        return False
    try:
        with conn.cursor() as cur:
            # formatted: 可能在 year 目录下 {week_tag}_formatted.json
            formatted_file = data_dir / (week_tag + "_formatted.json")
            if formatted_file.is_file():
                payload = json.loads(formatted_file.read_text(encoding="utf-8"))
                val = json.dumps(payload, ensure_ascii=False)
                cur.execute(
                    """INSERT INTO formatted_data (year, week_tag, payload) VALUES (%s, %s, %s)
                       ON DUPLICATE KEY UPDATE payload = VALUES(payload)""",
                    (year, week_tag, val),
                )
            week_dir = data_dir / week_tag
            if week_dir.is_dir():
                metrics_file = week_dir / "metrics_total.json"
                if metrics_file.is_file():
                    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
                    val = json.dumps(payload, ensure_ascii=False)
                    cur.execute(
                        """INSERT INTO metrics_total (year, week_tag, payload) VALUES (%s, %s, %s)
                           ON DUPLICATE KEY UPDATE payload = VALUES(payload)""",
                        (year, week_tag, val),
                    )
                # 产品维度 product_strategy（2.1 步拉取后产出）
                for key, stype in (("product_strategy_old", "old"), ("product_strategy_new", "new")):
                    f = week_dir / (key + ".json")
                    if f.is_file():
                        payload = json.loads(f.read_text(encoding="utf-8"))
                        val = json.dumps(payload, ensure_ascii=False)
                        cur.execute(
                            """INSERT INTO product_strategy (year, week_tag, strategy_type, payload) VALUES (%s, %s, %s, %s)
                               ON DUPLICATE KEY UPDATE payload = VALUES(payload)""",
                            (year, week_tag, stype, val),
                        )
                # 素材维度 creative_products（2.2 步拉取后产出）
                cp_file = week_dir / "creative_products.json"
                if cp_file.is_file():
                    payload = json.loads(cp_file.read_text(encoding="utf-8"))
                    val = json.dumps(payload, ensure_ascii=False)
                    cur.execute(
                        """INSERT INTO creative_products (year, week_tag, payload) VALUES (%s, %s, %s)
                           ON DUPLICATE KEY UPDATE payload = VALUES(payload)""",
                        (year, week_tag, val),
                    )
        conn.commit()
        refresh_weeks_index(conn, year, week_tag)
        return True
    except Exception:
        if conn:
            conn.rollback()
        return False
