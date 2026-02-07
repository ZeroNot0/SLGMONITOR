#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
扫描 advertisements/{年}/{周}/ 下的 product_type（strategy_old / strategy_new）与产品文件夹，
生成 frontend/data/{年}/{周}/creative_products.json，供素材维度产品下拉与路径拼接使用。
目录结构：advertisements/{year}/{week_tag}/{product_type}/{app_id}_{product_name}/json/{app_id}_{region}.json
"""
import json
import argparse
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
try:
    from app.app_paths import get_data_root
    ADS_DIR = get_data_root() / "advertisements"
except Exception:
    ADS_DIR = BASE_DIR / "advertisements"


def _get_data_dir() -> Path:
    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata).expanduser().resolve() / "SLGMonitor" / "frontend" / "data"
    try:
        from app.app_paths import get_data_root
        return get_data_root() / "frontend" / "data"
    except Exception:
        return Path(__file__).resolve().parent / "data"


DATA_DIR = _get_data_dir()

# 地区与 JSON 文件名后缀一致（fetch_ad_creatives 写入）
REGIONS = ["亚洲T1", "欧美T1", "T2", "T3"]
PRODUCT_TYPES = ["strategy_old", "strategy_new"]


def build_index_for_week(year: str, week_tag: str) -> dict:
    """扫描 advertisements/{year}/{week_tag}/ 生成该周的产品列表。"""
    base = ADS_DIR / year / week_tag
    if not base.is_dir():
        return {"week_tag": week_tag, "strategy_old": [], "strategy_new": []}

    result = {"week_tag": week_tag, "strategy_old": [], "strategy_new": []}
    for ptype in PRODUCT_TYPES:
        type_dir = base / ptype
        if not type_dir.is_dir():
            continue
        seen = set()
        for folder in type_dir.iterdir():
            if not folder.is_dir():
                continue
            folder_name = folder.name
            # folder 格式: app_id_Product Name（可能含冒号、空格等）
            parts = folder_name.split("_", 1)
            if len(parts) != 2:
                continue
            app_id, product_name = parts[0], parts[1]
            json_dir = folder / "json"
            if not json_dir.is_dir():
                continue
            # 至少有一个地区 JSON 才列入
            region_files = [f.name for f in json_dir.iterdir() if f.suffix == ".json"]
            if not region_files:
                continue
            key = (app_id, product_name)
            if key in seen:
                continue
            seen.add(key)
            result[ptype].append({
                "folder": folder_name,
                "app_id": app_id,
                "product_name": product_name,
                "display": product_name,
            })
    return result


def main():
    parser = argparse.ArgumentParser(description="生成素材维度产品索引 creative_products.json")
    parser.add_argument("--year", type=str, help="年份，如 2026")
    parser.add_argument("--week", type=str, help="周标签，如 0119-0125")
    parser.add_argument("--all", action="store_true", help="扫描 advertisements 下所有年/周并写入对应 data 目录")
    args = parser.parse_args()

    if args.all:
        if not ADS_DIR.is_dir():
            print("advertisements 目录不存在")
            return
        for year_dir in sorted(ADS_DIR.iterdir()):
            if not year_dir.is_dir():
                continue
            year = year_dir.name
            for week_dir in sorted(year_dir.iterdir()):
                if not week_dir.is_dir():
                    continue
                week_tag = week_dir.name
                index = build_index_for_week(year, week_tag)
                out_dir = DATA_DIR / year / week_tag
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / "creative_products.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(index, f, ensure_ascii=False, indent=2)
                print(f"已写: {out_path}")
        return

    if not args.year or not args.week:
        parser.error("请指定 --year 与 --week，或使用 --all 扫描全部")
        return
    index = build_index_for_week(args.year, args.week)
    out_dir = DATA_DIR / args.year / args.week
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "creative_products.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"已写: {out_path}")


if __name__ == "__main__":
    main()
