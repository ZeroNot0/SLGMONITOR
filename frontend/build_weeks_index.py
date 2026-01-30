#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
扫描 frontend/data/{年}/ 下的 {周}_formatted.json 与 {周}/ 目录，
生成 frontend/data/weeks_index.json，供前端左侧边栏「年/周」选择使用。
同时计算并写入 data_range（数据起止日期），供前端「数据时间」等展示在跑完脚本后自动更新。
"""
import json
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WEEKS_INDEX_FILE = DATA_DIR / "weeks_index.json"

# 周标签格式: MMDD-MMDD，如 1201-1207、1229-0104（跨年）
WEEK_TAG_PATTERN = re.compile(r"^(\d{2})(\d{2})-(\d{2})(\d{2})$")


def week_tag_to_dates(year: str, week_tag: str):
    """
    将 (年, 周标签) 转为 (起始日 ISO, 结束日 ISO)。
    周标签如 1201-1207 -> 12月1日~12月7日；1229-0104 -> 12月29日~次年1月4日。
    """
    m = WEEK_TAG_PATTERN.match(week_tag.strip())
    if not m:
        return None, None
    sm, sd = int(m.group(1)), int(m.group(2))
    em, ed = int(m.group(3)), int(m.group(4))
    y = int(year)
    start_iso = f"{y:04d}-{sm:02d}-{sd:02d}"
    end_year = y + 1 if em < sm else y
    end_iso = f"{end_year:04d}-{em:02d}-{ed:02d}"
    return start_iso, end_iso


def compute_data_range(index: dict):
    """
    根据 weeks index 计算全量数据的起止日期，返回 {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}。
    """
    if not index:
        return None
    all_starts = []
    all_ends = []
    for year in sorted(index.keys()):
        for week_tag in sorted(index[year]):
            s, e = week_tag_to_dates(year, week_tag)
            if s:
                all_starts.append(s)
            if e:
                all_ends.append(e)
    if not all_starts or not all_ends:
        return None
    return {"start": min(all_starts), "end": max(all_ends)}


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
    data_range = compute_data_range(index)
    out = dict(index)
    if data_range:
        out["data_range"] = data_range
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(WEEKS_INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"✅ 已更新: {WEEKS_INDEX_FILE}" + (f" 数据时间: {data_range['start']} ~ {data_range['end']}" if data_range else ""))
    ensure_creative_products_for_all_weeks(index)
    for y, wl in sorted(index.items()):
        print(f"   {y}: {len(wl)} 周 {wl[:3]}{'...' if len(wl) > 3 else ''}")


if __name__ == "__main__":
    main()
