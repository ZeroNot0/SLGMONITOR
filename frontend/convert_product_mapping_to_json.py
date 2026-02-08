#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 mapping/产品归属.xlsx 转为 frontend/data/product_theme_style_mapping.json，
供产品详情页按 Unified ID 取【题材】和【画风】。

输出格式：{ "byUnifiedId": { "id1": { "题材", "画风" }, ... }, "byProductName": { "产品名1": { "题材", "画风" }, ... } }
前端优先按 Unified ID 查 byUnifiedId，无则按产品名查 byProductName，保证无论从哪进入产品详情题材/画风都统一。

在 run_full_pipeline 步骤 5（前端数据更新）中每次执行。
"""
import json
import argparse
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
MAPPING_XLSX = BASE_DIR / "mapping" / "产品归属.xlsx"


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
OUT_JSON = DATA_DIR / "product_theme_style_mapping.json"


def _str(v):
    if v is None:
        return ""
    if hasattr(v, "strip"):
        return str(v).strip()
    return str(v).strip() if v else ""


def run() -> bool:
    if not MAPPING_XLSX.exists() or MAPPING_XLSX.stat().st_size == 0:
        print(f"  ⏭ 未找到或为空: {MAPPING_XLSX.relative_to(BASE_DIR)}，跳过题材/画风映射")
        return True
    try:
        import pandas as pd
    except ImportError:
        print("  ⚠️ 需要 pandas：pip install pandas openpyxl，跳过题材/画风映射")
        return True
    try:
        df = pd.read_excel(MAPPING_XLSX)
    except Exception as e:
        print(f"  ⚠️ 无法读取 {MAPPING_XLSX.name}（{e}），跳过题材/画风映射")
        return True
    if df.empty or len(df.columns) < 2:
        print(f"  ⏭ 表为空或列不足，跳过题材/画风映射")
        return True
    # 列名：优先 "Unified ID"，否则 "Unified Name"（与 step2 一致）；题材、画风
    cols = [c for c in df.columns if c is not None and str(c).strip()]
    key_col = None
    for name in ("Unified ID", "Unified Name", "Unified id", "unified id"):
        for c in cols:
            if str(c).strip() == name:
                key_col = c
                break
        if key_col is not None:
            break
    theme_col = None
    style_col = None
    product_col = None
    for c in cols:
        s = str(c).strip()
        if s in ("题材", "题材标签"):
            theme_col = c
        if s in ("画风", "画风标签"):
            style_col = c
        if s == "产品归属":
            product_col = c
    if key_col is None:
        # 与 step2 一致：B 列(索引 1) 为 Unified Name
        if df.shape[1] > 1:
            key_col = df.columns[1]
        else:
            print("  ⏭ 未找到 Unified ID/Unified Name 列，跳过题材/画风映射")
            return True
    by_unified_id = {}
    by_product_name = {}
    for _, row in df.iterrows():
        key_val = row.get(key_col)
        key_val = _str(key_val)
        if not key_val:
            continue
        theme_val = _str(row.get(theme_col)) if theme_col is not None else ""
        style_val = _str(row.get(style_col)) if style_col is not None else ""
        entry = {"题材": theme_val, "画风": style_val}
        by_unified_id[key_val] = entry
        if product_col is not None:
            pname = _str(row.get(product_col))
            if pname:
                by_product_name[pname] = entry
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out = {"byUnifiedId": by_unified_id, "byProductName": by_product_name}
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    try:
        display_path = str(OUT_JSON.relative_to(BASE_DIR))
    except Exception:
        display_path = str(OUT_JSON)
    print(f"  已生成题材/画风映射: {display_path}（byUnifiedId {len(by_unified_id)} 条, byProductName {len(by_product_name)} 条）")
    return True


def main():
    parser = argparse.ArgumentParser(description="将 mapping/产品归属.xlsx 转为题材/画风 JSON")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写入")
    args = parser.parse_args()
    if args.dry_run:
        if MAPPING_XLSX.exists():
            print(f"输入: {MAPPING_XLSX}")
            print(f"输出: {OUT_JSON}")
        else:
            print(f"未找到: {MAPPING_XLSX}")
        return
    run()


if __name__ == "__main__":
    main()
