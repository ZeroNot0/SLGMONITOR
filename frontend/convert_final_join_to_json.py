#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 final_join 下的目标产品表（各 T 度市场获量）转为 JSON，供前端【产品维度】展示。
输入：final_join/{年}/{周}/target_strategy_old_with_ads_all.xlsx、target_strategy_new_with_ads_all.xlsx
输出：frontend/data/{年}/{周}/product_strategy_old.json、product_strategy_new.json
"""
import json
import argparse
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent

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


def run(year: int, week_tag: str) -> None:
    final_dir = BASE_DIR / "final_join" / str(year) / week_tag
    out_dir = BASE_DIR / "frontend" / "data" / str(year) / week_tag
    out_dir.mkdir(parents=True, exist_ok=True)

    for key, filename in [
        ("old", "target_strategy_old_with_ads_all.xlsx"),
        ("new", "target_strategy_new_with_ads_all.xlsx"),
    ]:
        excel_path = final_dir / filename
        if not excel_path.exists():
            print(f"跳过（不存在）: {excel_path}")
            continue
        json_path = out_dir / f"product_strategy_{key}.json"
        convert_sheet_to_json(excel_path, json_path)
        print(f"已生成: {json_path}")


def main():
    parser = argparse.ArgumentParser(description="将 final_join 表转为前端产品维度 JSON")
    parser.add_argument("--year", type=int, required=True, help="年份，如 2025")
    parser.add_argument("--week", type=str, required=True, help="周标签，如 1201-1207")
    args = parser.parse_args()
    run(args.year, args.week)


if __name__ == "__main__":
    main()
