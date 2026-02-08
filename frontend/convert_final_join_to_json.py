#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 final_join 下的目标产品表（各 T 度市场获量）转为 JSON，供前端【产品维度】展示。
输入：final_join/{年}/{周}/target_strategy_old_with_ads_all.xlsx、target_strategy_new_with_ads_all.xlsx
输出：frontend/data/{年}/{周}/product_strategy_old.json、product_strategy_new.json
"""
import json
import argparse
import os
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent


def _get_data_dir() -> Path:
    override = os.environ.get("SLG_MONITOR_DATA_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve() / "frontend" / "data"
    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata).expanduser().resolve() / "SLGMonitor" / "frontend" / "data"
    try:
        from app.app_paths import get_data_root
        return get_data_root() / "frontend" / "data"
    except Exception:
        return Path(__file__).resolve().parent / "data"


DATA_DIR = _get_data_dir()

# 列名映射：xlsx 列名 -> 前端展示名（与产品维度页面一致）
COLUMN_DISPLAY = {
    "亚洲T1_安装": "亚洲 T1 市场获量",
    "欧美T1_安装": "欧美 T1 市场获量",
    "T2_安装": "T2 市场获量",
    "T3_安装": "T3 市场获量",
    "Downloads (PoP Growth %)": "周安装变动",
}

# 产品维度页面展示列（顺序）；Unified ID 用于前端按产品唯一标识聚合/匹配
PRODUCT_DIMENSION_COLUMNS = [
    "产品归属",
    "Unified ID",
    "公司归属",
    "第三方记录最早上线时间",
    "当周周安装",
    "上周周安装",
    "周安装变动",
    "亚洲 T1 市场获量",
    "欧美 T1 市场获量",
    "T2 市场获量",
    "T3 市场获量",
]

# 4 个地区获量列：第一步未执行 step3 时从 target 生成 JSON 时填 0
REGION_COLUMNS = ["亚洲 T1 市场获量", "欧美 T1 市场获量", "T2 市场获量", "T3 市场获量"]


def convert_sheet_to_json(excel_path: Path, json_path: Path) -> None:
    df = pd.read_excel(excel_path)
    df = df.rename(columns=COLUMN_DISPLAY)
    # 只保留产品维度需要的列（存在的列）
    want = [c for c in PRODUCT_DIMENSION_COLUMNS if c in df.columns]
    if want:
        df = df[want]
    headers = list(df.columns)
    rows = []
    for _, r in df.iterrows():
        row = []
        for v in r:
            if pd.isna(v):
                row.append("")
            elif isinstance(v, (int, float)) and not isinstance(v, bool):
                row.append(int(v) if v == int(v) else v)
            else:
                row.append(str(v))
        rows.append(row)
    data = {"headers": headers, "rows": rows}
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_empty_product_strategy_json(json_path: Path) -> None:
    """当该周无爆量目标产品时仍写入带表头的空 JSON，前端可显示表头而非整页空白。"""
    data = {"headers": list(PRODUCT_DIMENSION_COLUMNS), "rows": []}
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _target_row_to_product_dimension_row(r, headers: list) -> list:
    """把 target 表一行转成产品维度顺序的数组，缺列填空，4 个地区列填 0。"""
    row = []
    for col in headers:
        if col in REGION_COLUMNS:
            row.append(0)
        elif col in r.index:
            v = r[col]
            if pd.isna(v):
                row.append("")
            elif isinstance(v, (int, float)) and not isinstance(v, bool):
                row.append(int(v) if v == int(v) else v)
            else:
                row.append(str(v))
        else:
            row.append("")
    return row


def convert_target_to_json(target_path: Path, json_path: Path) -> bool:
    """
    当 final_join 不存在时，直接从 target 表生成产品维度 JSON：
    前 6 列 + Unified ID 从 target 填充，4 个地区获量列填 0（第一步未请求分地区数据时）。
    """
    if not target_path.exists():
        return False
    try:
        df = pd.read_excel(target_path)
    except Exception:
        return False
    if df.empty:
        return False
    headers = list(PRODUCT_DIMENSION_COLUMNS)
    rows = []
    for _, r in df.iterrows():
        rows.append(_target_row_to_product_dimension_row(r, headers))
    data = {"headers": headers, "rows": rows}
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return True


def run(year: int, week_tag: str) -> None:
    final_dir = BASE_DIR / "final_join" / str(year) / week_tag
    target_dir = BASE_DIR / "target" / str(year) / week_tag / "strategy_target"
    out_dir = DATA_DIR / str(year) / week_tag
    out_dir.mkdir(parents=True, exist_ok=True)

    for key, filename in [
        ("old", "target_strategy_old_with_ads_all.xlsx"),
        ("new", "target_strategy_new_with_ads_all.xlsx"),
    ]:
        excel_path = final_dir / filename
        json_path = out_dir / f"product_strategy_{key}.json"
        if excel_path.exists():
            convert_sheet_to_json(excel_path, json_path)
            print(f"已生成: {json_path}")
            continue
        # final_join 不存在（第一步未请求地区数据）：用 target 表填 6 列，4 个地区获量填 0
        target_filename = "target_strategy_old.xlsx" if key == "old" else "target_strategy_new.xlsx"
        target_path = target_dir / target_filename
        if convert_target_to_json(target_path, json_path):
            print(f"已生成（来自 target，4 地区获量为 0）: {json_path}")
            continue
        # 无 target 或 target 为空时，只写表头
        write_empty_product_strategy_json(json_path)
        print(f"已生成（空表）: {json_path}")


def main():
    parser = argparse.ArgumentParser(description="将 final_join 表转为前端产品维度 JSON")
    parser.add_argument("--year", type=int, required=True, help="年份，如 2025")
    parser.add_argument("--week", type=str, required=True, help="周标签，如 1201-1207")
    args = parser.parse_args()
    run(args.year, args.week)


if __name__ == "__main__":
    main()
