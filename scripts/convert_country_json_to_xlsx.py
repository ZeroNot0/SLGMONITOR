#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将地区数据 JSON 转成与表格一致的 xlsx，写入 countrydata 目录（仿照 advertisements 结构）。

目录结构（仿照 advertisements：年/周/strategy_old|strategy_new）：
  countiesdata/{年}/{周}/strategy_old/json、xlsx
  countiesdata/{年}/{周}/strategy_new/json、xlsx

列顺序：app_id, country, date, android_units, android_revenue, iphone_units,
       iphone_revenue, unified_units, unified_revenue, ipad_units, ipad_revenue
日期格式：YYYY-MM-DD（避免 Excel 显示 ######）

用法（建议在 deeppython 环境下运行）：
  conda activate deeppython
  # 迁移：把 request/country_data/json 迁到 countiesdata/{年}/{周}/strategy_old 与 strategy_new（需指定 --year --week）
  python scripts/convert_country_json_to_xlsx.py --migrate --year 2025 --week 1201-1207
  # 指定周：读 countiesdata/{年}/{周}/strategy_old 与 strategy_new 下 json，写 xlsx
  python scripts/convert_country_json_to_xlsx.py --year 2025 --week 1201-1207
"""

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Optional

try:
    import pandas as pd
except ImportError:
    pd = None  # 需要: pip install pandas openpyxl

BASE_DIR = Path(__file__).resolve().parent.parent
# 仿照 advertisements：countiesdata/{年}/{周}/json 与 xlsx
COUNTRYDATA_BASE = BASE_DIR / "countiesdata"
LEGACY_JSON_DIR = BASE_DIR / "request" / "country_data" / "json"
LEGACY_XLSX_DIR = BASE_DIR / "request" / "country_data" / "xlsx"

COLUMN_ORDER = [
    "app_id",
    "country",
    "date",
    "android_units",
    "android_revenue",
    "iphone_units",
    "iphone_revenue",
    "unified_units",
    "unified_revenue",
    "ipad_units",
    "ipad_revenue",
]


def format_date(val):
    """ISO 日期 → YYYY-MM-DD，便于 Excel 显示。"""
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    if "T" in s:
        s = s.split("T")[0]
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s


def load_json_rows(path: Path):
    """从 JSON 文件加载为 list[dict]，支持顶层 list 或 { \"data\": [...] }。"""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and "data" in raw:
        return raw.get("data") or []
    return []


def normalize_row(row: dict, app_id: str) -> dict:
    """补全列、格式化日期，返回一行字典。"""
    out = {}
    for col in COLUMN_ORDER:
        v = row.get(col)
        if v is None and col in (
            "android_units", "android_revenue", "iphone_units", "iphone_revenue",
            "unified_units", "unified_revenue", "ipad_units", "ipad_revenue",
        ):
            out[col] = 0
        elif col == "date":
            out[col] = format_date(v)
        elif col == "app_id" and (v is None or v == ""):
            out[col] = app_id
        else:
            out[col] = v if v is not None else ""
    return out


def convert_one(app_id: str, rows: list, xlsx_dir: Path) -> bool:
    """将多行数据写成 {app_id}.xlsx。"""
    if pd is None:
        print("  需要安装 pandas 与 openpyxl: pip install pandas openpyxl")
        return False
    if not rows:
        print(f"  {app_id}: 无数据，跳过")
        return True
    xlsx_dir.mkdir(parents=True, exist_ok=True)
    normalized = [normalize_row(r, app_id) for r in rows if isinstance(r, dict)]
    df = pd.DataFrame(normalized)
    df = df[[c for c in COLUMN_ORDER if c in df.columns]]
    path = xlsx_dir / f"{app_id}.xlsx"
    df.to_excel(path, index=False)
    print(f"  已写: {path} ({len(df)} 行)")
    return True


def run_convert(
    json_dir: Path,
    xlsx_dir: Path,
    only_app_id: Optional[str] = None,
    combined: bool = False,
):
    """从 json_dir 读所有 *.json，转成 xlsx 写到 xlsx_dir。"""
    if not json_dir.exists():
        print(f"目录不存在: {json_dir}")
        return False
    files = sorted(json_dir.glob("*.json"))
    if only_app_id:
        only_app_id = only_app_id.strip()
        files = [f for f in files if f.stem == only_app_id]
        if not files:
            print(f"未找到 app_id 对应 JSON: {only_app_id}")
            return False
    all_rows = []
    for path in files:
        app_id = path.stem
        rows = load_json_rows(path)
        if not rows:
            print(f"  {app_id}: 无数据，跳过")
            continue
        convert_one(app_id, rows, xlsx_dir)
        for r in rows:
            if isinstance(r, dict):
                all_rows.append(normalize_row(r, app_id))
    if combined and all_rows and pd is not None:
        try:
            xlsx_dir.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame(all_rows)
            df = df[[c for c in COLUMN_ORDER if c in df.columns]]
            combined_path = xlsx_dir / "all_apps_country_data.xlsx"
            df.to_excel(combined_path, index=False)
            print(f"  已写合并表: {combined_path} ({len(df)} 行)")
        except Exception as e:
            print(f"  写合并表失败: {e}")
    return True


def migrate_legacy_to_countrydata(year: int, week_tag: str, combined: bool = False):
    """把 request/country_data/json 下所有 json 复制到 countiesdata/{年}/{周}/strategy_old 与 strategy_new，并转 xlsx。"""
    if not LEGACY_JSON_DIR.exists():
        print(f"存量目录不存在: {LEGACY_JSON_DIR}")
        return False
    files = list(LEGACY_JSON_DIR.glob("*.json"))
    for product_type in ("strategy_old", "strategy_new"):
        json_dir = COUNTRYDATA_BASE / str(year) / week_tag / product_type / "json"
        xlsx_dir = COUNTRYDATA_BASE / str(year) / week_tag / product_type / "xlsx"
        json_dir.mkdir(parents=True, exist_ok=True)
        for f in files:
            shutil.copy2(f, json_dir / f.name)
        print(f"  已复制 {len(files)} 个 JSON 到 countiesdata/{year}/{week_tag}/{product_type}/json/，转 xlsx...")
        run_convert(json_dir, xlsx_dir, combined=combined and product_type == "strategy_old")
    return True


def main():
    parser = argparse.ArgumentParser(description="将地区数据 JSON 转成 xlsx，写入 countrydata")
    parser.add_argument("--migrate", action="store_true", help="从 request/country_data/json 迁到 countiesdata/{年}/{周}/strategy_old 与 strategy_new（需同时指定 --year --week）")
    parser.add_argument("--year", type=int, help="年份，与 --week 一起指定时读写 countiesdata/{年}/{周}/")
    parser.add_argument("--week", dest="week_tag", help="周标签如 1201-1207，与 --year 一起时读写 countiesdata/{年}/{周}/")
    parser.add_argument("--app_id", help="只处理该 app_id")
    parser.add_argument("--combined", action="store_true", help="额外输出合并表 all_apps_country_data.xlsx")
    args = parser.parse_args()

    if args.migrate:
        if args.year is None or not args.week_tag:
            print("  --migrate 需同时指定 --year 与 --week")
            sys.exit(1)
        ok = migrate_legacy_to_countrydata(args.year, args.week_tag, combined=args.combined)
        sys.exit(0 if ok else 1)

    if args.year is not None and args.week_tag:
        ok = True
        for product_type in ("strategy_old", "strategy_new"):
            json_dir = COUNTRYDATA_BASE / str(args.year) / args.week_tag / product_type / "json"
            xlsx_dir = COUNTRYDATA_BASE / str(args.year) / args.week_tag / product_type / "xlsx"
            if json_dir.exists():
                print(f"  转换 {product_type}...")
                ok = run_convert(json_dir, xlsx_dir, only_app_id=args.app_id, combined=args.combined) and ok
            else:
                print(f"  ⏭ 跳过 {product_type}（目录不存在: {json_dir}）")
        sys.exit(0 if ok else 1)

    # 默认：仅转换 request/country_data/json → xlsx（legacy）
    ok = run_convert(LEGACY_JSON_DIR, LEGACY_XLSX_DIR, only_app_id=args.app_id, combined=args.combined)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
