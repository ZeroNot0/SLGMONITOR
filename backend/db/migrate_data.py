#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将现有 frontend/data/*.json、mapping/*.xlsx、labels/*.xlsx、deploy/auth_users.json 导入 MySQL。
在项目根目录执行：python -m backend.db.migrate_data
或：cd backend/db && python migrate_data.py
可设置环境变量 MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE；
或用 root：MYSQL_USER=root MYSQL_PASSWORD=你的root密码 python -m backend.db.migrate_data
"""
import argparse
import json
import sys
from pathlib import Path

# 保证能从项目根或 backend/db 目录运行
_ROOT = Path(__file__).resolve().parent.parent.parent
if _ROOT not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    import pymysql
except ImportError:
    print("请先安装 pymysql: pip install pymysql")
    sys.exit(1)

try:
    from backend.db.config import BASE_DIR, get_mysql_config
except ImportError:
    BASE_DIR = _ROOT
    def get_mysql_config():
        import os
        return {
            "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
            "port": int(os.environ.get("MYSQL_PORT", "3306")),
            "user": os.environ.get("MYSQL_USER", "slg_monitor"),
            "password": os.environ.get("MYSQL_PASSWORD", ""),
            "database": os.environ.get("MYSQL_DATABASE", "slg_monitor"),
            "charset": "utf8mb4",
        }

FRONTEND_DATA = BASE_DIR / "frontend" / "data"
MAPPING_DIR = BASE_DIR / "mapping"
LABELS_DIR = BASE_DIR / "labels"
AUTH_USERS_PATH = BASE_DIR / "deploy" / "auth_users.json"

BASETABLE_SOURCES = {
    "product_mapping": MAPPING_DIR / "产品归属.xlsx",
    "company_mapping": MAPPING_DIR / "公司归属.xlsx",
    "theme_label": LABELS_DIR / "题材标签表.xlsx",
    "gameplay_label": LABELS_DIR / "玩法标签表.xlsx",
    "art_style_label": LABELS_DIR / "画风标签表.xlsx",
}


def _excel_to_headers_rows(path):
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
            if getattr(pd, "isna", lambda x: x != x)(v):
                row.append("")
            elif isinstance(v, (int, float)) and not isinstance(v, bool):
                row.append(int(v) if v == int(v) else v)
            else:
                row.append(str(v).strip() if v is not None else "")
        rows.append(row)
    return headers, rows


def run_migration(mysql_config):
    conn = pymysql.connect(
        host=mysql_config["host"],
        port=mysql_config["port"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
        charset=mysql_config.get("charset", "utf8mb4"),
    )
    try:
        with conn.cursor() as cur:
            # 1. weeks_index + data_range -> app_config
            weeks_path = FRONTEND_DATA / "weeks_index.json"
            if weeks_path.is_file():
                data = json.loads(weeks_path.read_text(encoding="utf-8"))
                val = json.dumps(data, ensure_ascii=False)
                cur.execute(
                    "INSERT INTO app_config (config_key, config_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)",
                    ("weeks_index", val),
                )
                if "data_range" in data:
                    cur.execute(
                        "INSERT INTO app_config (config_key, config_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)",
                        ("data_range", json.dumps(data["data_range"], ensure_ascii=False)),
                    )
                # year_weeks
                for year_s, weeks in data.items():
                    if year_s == "data_range" or not isinstance(weeks, list):
                        continue
                    try:
                        year = int(year_s)
                    except ValueError:
                        continue
                    for week_tag in weeks:
                        if isinstance(week_tag, str):
                            cur.execute(
                                "INSERT IGNORE INTO year_weeks (year, week_tag) VALUES (%s, %s)",
                                (year, week_tag),
                            )
                print("  [OK] app_config weeks_index, year_weeks")
            conn.commit()

            # 2. formatted_data
            for year_dir in FRONTEND_DATA.iterdir():
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                year = int(year_dir.name)
                for f in year_dir.iterdir():
                    if f.is_file() and f.name.endswith("_formatted.json"):
                        week_tag = f.name.replace("_formatted.json", "")
                        payload = json.loads(f.read_text(encoding="utf-8"))
                        val = json.dumps(payload, ensure_ascii=False)
                        cur.execute(
                            """INSERT INTO formatted_data (year, week_tag, payload) VALUES (%s, %s, %s)
                               ON DUPLICATE KEY UPDATE payload = VALUES(payload)""",
                            (year, week_tag, val),
                        )
            conn.commit()
            print("  [OK] formatted_data")

            # 3. product_strategy
            for year_dir in FRONTEND_DATA.iterdir():
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                year = int(year_dir.name)
                sub = year_dir / "dummy"
                for week_dir in year_dir.iterdir():
                    if not week_dir.is_dir():
                        continue
                    week_tag = week_dir.name
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
            conn.commit()
            print("  [OK] product_strategy")

            # 4. creative_products
            for year_dir in FRONTEND_DATA.iterdir():
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                year = int(year_dir.name)
                for week_dir in year_dir.iterdir():
                    if not week_dir.is_dir():
                        continue
                    week_tag = week_dir.name
                    f = week_dir / "creative_products.json"
                    if f.is_file():
                        payload = json.loads(f.read_text(encoding="utf-8"))
                        val = json.dumps(payload, ensure_ascii=False)
                        cur.execute(
                            """INSERT INTO creative_products (year, week_tag, payload) VALUES (%s, %s, %s)
                               ON DUPLICATE KEY UPDATE payload = VALUES(payload)""",
                            (year, week_tag, val),
                        )
            conn.commit()
            print("  [OK] creative_products")

            # 5. metrics_total
            for year_dir in FRONTEND_DATA.iterdir():
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                year = int(year_dir.name)
                for week_dir in year_dir.iterdir():
                    if not week_dir.is_dir():
                        continue
                    week_tag = week_dir.name
                    f = week_dir / "metrics_total.json"
                    if f.is_file():
                        payload = json.loads(f.read_text(encoding="utf-8"))
                        val = json.dumps(payload, ensure_ascii=False)
                        cur.execute(
                            """INSERT INTO metrics_total (year, week_tag, payload) VALUES (%s, %s, %s)
                               ON DUPLICATE KEY UPDATE payload = VALUES(payload)""",
                            (year, week_tag, val),
                        )
            conn.commit()
            print("  [OK] metrics_total")

            # 6. metrics_rank（全局一份，先删后插避免重复）
            global_rank = FRONTEND_DATA / "metrics_rank.json"
            if global_rank.is_file():
                payload = json.loads(global_rank.read_text(encoding="utf-8"))
                val = json.dumps(payload, ensure_ascii=False)
                cur.execute("DELETE FROM metrics_rank WHERE scope = 'global'")
                cur.execute(
                    "INSERT INTO metrics_rank (payload, scope) VALUES (%s, 'global')",
                    (val,),
                )
                conn.commit()
            print("  [OK] metrics_rank")

            # 7. new_products
            np_path = FRONTEND_DATA / "new_products.json"
            if np_path.is_file():
                payload = json.loads(np_path.read_text(encoding="utf-8"))
                val = json.dumps(payload, ensure_ascii=False)
                cur.execute(
                    "INSERT INTO new_products (id, payload) VALUES (1, %s) ON DUPLICATE KEY UPDATE payload = VALUES(payload)",
                    (val,),
                )
                conn.commit()
            print("  [OK] new_products")

            # 8. product_theme_style_mapping
            mapping_path = FRONTEND_DATA / "product_theme_style_mapping.json"
            if mapping_path.is_file():
                payload = json.loads(mapping_path.read_text(encoding="utf-8"))
                val = json.dumps(payload, ensure_ascii=False)
                cur.execute(
                    "INSERT INTO product_theme_style_mapping (id, payload) VALUES (1, %s) ON DUPLICATE KEY UPDATE payload = VALUES(payload)",
                    (val,),
                )
                conn.commit()
            print("  [OK] product_theme_style_mapping")

            # 9. basetable
            for name, xlsx_path in BASETABLE_SOURCES.items():
                headers, rows = _excel_to_headers_rows(xlsx_path)
                if headers or rows:
                    h_val = json.dumps(headers, ensure_ascii=False)
                    r_val = json.dumps(rows, ensure_ascii=False)
                    cur.execute(
                        """INSERT INTO basetable (name, headers, `rows`) VALUES (%s, %s, %s)
                           ON DUPLICATE KEY UPDATE headers = VALUES(headers), `rows` = VALUES(`rows`)""",
                        (name, h_val, r_val),
                    )
            conn.commit()
            print("  [OK] basetable")

            # 10. users（从 auth_users.json）
            if AUTH_USERS_PATH.is_file():
                data = json.loads(AUTH_USERS_PATH.read_text(encoding="utf-8"))
                users = data.get("users") or []
                for u in users:
                    username = (u.get("username") or "").strip()
                    if not username:
                        continue
                    salt = (u.get("salt") or "").strip()
                    h = (u.get("hash") or "").strip()
                    role = (u.get("role") or "user").strip() or "user"
                    status = (u.get("status") or "pending").strip() or "pending"
                    cur.execute(
                        """INSERT INTO users (username, salt, password_hash, role, status)
                           VALUES (%s, %s, %s, %s, %s)
                           ON DUPLICATE KEY UPDATE salt = VALUES(salt), password_hash = VALUES(password_hash), role = VALUES(role), status = VALUES(status)""",
                        (username, salt, h, role, status),
                    )
                conn.commit()
            print("  [OK] users")
    finally:
        conn.close()
    print("迁移完成。")


def main():
    parser = argparse.ArgumentParser(description="将 frontend/data 与 mapping 等导入 MySQL")
    parser.add_argument("--mysql-user", default=None, help="MySQL 用户名，默认从 MYSQL_USER 或 slg_monitor")
    parser.add_argument("--mysql-password", default=None, help="MySQL 密码，默认从 MYSQL_PASSWORD")
    parser.add_argument("--mysql-host", default=None)
    parser.add_argument("--mysql-port", type=int, default=None)
    parser.add_argument("--mysql-database", default=None)
    args = parser.parse_args()
    cfg = get_mysql_config()
    if args.mysql_user is not None:
        cfg["user"] = args.mysql_user
    if args.mysql_password is not None:
        cfg["password"] = args.mysql_password
    if args.mysql_host is not None:
        cfg["host"] = args.mysql_host
    if args.mysql_port is not None:
        cfg["port"] = args.mysql_port
    if args.mysql_database is not None:
        cfg["database"] = args.mysql_database
    # 允许空密码（本地 Homebrew MySQL 等 root 常无密码）
    run_migration(cfg)


if __name__ == "__main__":
    main()
