#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
扫描 frontend/data/{年}/ 下的 {周}_formatted.json 与 {周}/ 目录，
生成 frontend/data/weeks_index.json，供前端左侧边栏「年/周」选择使用。
新增一周数据并生成对应 formatted 或子目录后，运行本脚本即可在侧栏看到新周。
"""
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WEEKS_INDEX_FILE = DATA_DIR / "weeks_index.json"


def build_weeks_index():
    """扫描 data/{year}/ 得到所有年与周，返回 {year: [week_tag, ...]} 并排序。"""
    index = {}
    if not DATA_DIR.is_dir():
        return index
    for year_dir in sorted(DATA_DIR.iterdir()):
        if not year_dir.is_dir():
            continue
        year = year_dir.name
        if not year.isdigit() or len(year) != 4:
            continue
        weeks = set()
        for item in year_dir.iterdir():
            if item.is_file() and item.name.endswith("_formatted.json"):
                week = item.name[: -len("_formatted.json")]
                weeks.add(week)
            elif item.is_dir():
                weeks.add(item.name)
        if weeks:
            index[year] = sorted(weeks)
    return index


def ensure_creative_products_for_all_weeks(index: dict) -> None:
    """为侧栏中每一周确保 data/{年}/{周}/creative_products.json 存在，无则写空，使素材维度与公司/产品维度周期一致。"""
    for year, weeks in index.items():
        for week in weeks:
            out_dir = DATA_DIR / year / week
            out_file = out_dir / "creative_products.json"
            if out_file.exists():
                continue
            out_dir.mkdir(parents=True, exist_ok=True)
            empty = {"week_tag": week, "strategy_old": [], "strategy_new": []}
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(empty, f, ensure_ascii=False, indent=2)
            print(f"  补全空索引: {out_file.relative_to(DATA_DIR)}")


def main():
    index = build_weeks_index()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(WEEKS_INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"✅ 已更新: {WEEKS_INDEX_FILE}")
    ensure_creative_products_for_all_weeks(index)
    for y, wl in sorted(index.items()):
        print(f"   {y}: {len(wl)} 周 {wl[:3]}{'...' if len(wl) > 3 else ''}")


if __name__ == "__main__":
    main()
