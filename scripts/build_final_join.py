#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
目标表 + 地区数据（按 T 度汇总）→ final_join，产出四个市场获量：亚洲T1、欧美T1、T2、T3。

要使「三个/四个市场获量」有数，必须满足：
1. 先执行步骤 3（拉取地区数据），保证 request/country_data/json/{app_id}.json 存在；
2. target 表里「Unified ID」列是完整的 24 位 ID 字符串（若被 Excel 存成数字会变成科学计数法，无法匹配 JSON 文件名，获量会为 0）。

输入：
- target/{年}/{周}/strategy_target/target_strategy_old.xlsx、target_strategy_new.xlsx
- request/country_data/json/{app_id}.json（或 xlsx）
- mapping/市场T度.csv
输出：
- final_join/{年}/{周}/target_strategy_old_with_ads_all.xlsx、target_strategy_new_with_ads_all.xlsx
- frontend/data/{年}/{周}/product_strategy_old.json、product_strategy_new.json（自动调用 convert_final_join_to_json，供产品维度页使用）
"""
import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
TARGET_BASE = BASE_DIR / "target"
# 地区数据优先从 countiesdata（仿照 advertisements）读取，再回退到 request/country_data
COUNTIESDATA_BASE = BASE_DIR / "countiesdata"
LEGACY_COUNTRY_JSON = BASE_DIR / "request" / "country_data" / "json"
LEGACY_COUNTRY_XLSX = BASE_DIR / "request" / "country_data" / "xlsx"
FINAL_JOIN_BASE = BASE_DIR / "final_join"
MAPPING_CSV = BASE_DIR / "mapping" / "市场T度.csv"

# 亚洲 T1 排除 CN（与文档一致）
REGIONS = ["亚洲T1", "欧美T1", "T2", "T3"]
INSTALL_COLS = [f"{r}_安装" for r in REGIONS]
REVENUE_COLS = [f"{r}_流水" for r in REGIONS]


def load_country_to_tier():
    """country -> T度，未在表中的为 T3；亚洲 T1 不含 CN。"""
    tier = {}
    if MAPPING_CSV.exists():
        with open(MAPPING_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                c = (row.get("country") or "").strip().upper()
                t = (row.get("T度") or "").strip()
                if c and t:
                    tier[c] = t
    for c in ["CN"]:
        if c in tier and tier[c] == "亚洲T1":
            del tier[c]  # 亚洲 T1 排除 CN
    return tier


def _country_data_dirs(year: int, week_tag: str, product_type: str):
    """地区数据查找顺序：countiesdata/{年}/{周}/strategy_old|strategy_new → request/country_data。"""
    return [
        (COUNTIESDATA_BASE / str(year) / week_tag / product_type / "json", COUNTIESDATA_BASE / str(year) / week_tag / product_type / "xlsx"),
        (LEGACY_COUNTRY_JSON, LEGACY_COUNTRY_XLSX),
    ]


def load_country_data_for_app(app_id: str, year: int = None, week_tag: str = None, product_type: str = None):
    """加载单个 app 的国家维度数据，返回 list[dict]（含 country, unified_units, unified_revenue）。优先 countiesdata/{年}/{周}/strategy_old|strategy_new。"""
    data = []
    if year and week_tag and product_type:
        dirs_to_try = _country_data_dirs(year, week_tag, product_type)
    else:
        dirs_to_try = [(LEGACY_COUNTRY_JSON, LEGACY_COUNTRY_XLSX)]
    for json_dir, xlsx_dir in dirs_to_try:
        json_path = json_dir / f"{app_id}.json"
        xlsx_path = xlsx_dir / f"{app_id}.xlsx"
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, list):
                data = raw
            elif isinstance(raw, dict) and "data" in raw:
                data = raw.get("data") or []
            break
        elif xlsx_path.exists():
            df = pd.read_excel(xlsx_path)
            if not df.empty and "country" in df.columns:
                data = df.to_dict("records")
            break
    for row in data:
        if not isinstance(row, dict):
            continue
        c = row.get("country")
        if pd.isna(c) or c is None:
            continue
        row["country"] = str(c).strip().upper()
        if "unified_units" not in row:
            row["unified_units"] = 0
        if "unified_revenue" not in row:
            row["unified_revenue"] = 0
    return data


def aggregate_by_tier(rows: list, country_to_tier: dict) -> dict:
    """按 T 度汇总安装与流水。返回 {亚洲T1_安装, 欧美T1_安装, T2_安装, T3_安装, 亚洲T1_流水, ...}"""
    sums = {f"{r}_安装": 0 for r in REGIONS}
    for k in REGIONS:
        sums[f"{k}_流水"] = 0
    for row in rows:
        country = (row.get("country") or "").strip().upper()
        tier = country_to_tier.get(country, "T3")
        if tier not in REGIONS:
            tier = "T3"
        u = int(row.get("unified_units") or 0)
        r = int(row.get("unified_revenue") or 0)
        sums[f"{tier}_安装"] += u
        sums[f"{tier}_流水"] += r
    return sums


def build_tier_df(app_ids: list, country_to_tier: dict, year: int = None, week_tag: str = None, product_type: str = None) -> pd.DataFrame:
    """对每个 app_id 加载 country_data、按 T 度汇总，返回 df 列 [app_id, 亚洲T1_安装, ...]。"""
    rows = []
    for app_id in app_ids:
        app_id = (app_id or "").strip()
        if not app_id:
            continue
        raw = load_country_data_for_app(app_id, year=year, week_tag=week_tag, product_type=product_type)
        agg = aggregate_by_tier(raw, country_to_tier)
        agg["app_id"] = app_id
        rows.append(agg)
    if not rows:
        return pd.DataFrame(columns=["app_id"] + INSTALL_COLS + REVENUE_COLS)
    return pd.DataFrame(rows)


def _normalize_app_id(val):
    """Excel 可能把 Unified ID 存成数字/科学计数法，转为字符串；24 位 hex 原样保留。"""
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if s.lower().startswith("nan") or s == "":
        return ""
    # 科学计数法（如 6.6e+23）转 int 会丢精度，无法匹配 JSON 文件名，故不转换
    if "e+" in s.lower() or "e-" in s.lower():
        return s
    # 已是 24 位 hex 则不动
    if len(s) == 24 and all(c in "0123456789abcdef" for c in s.lower()):
        return s
    # 去掉 .0 等
    try:
        n = float(s)
        if n == int(n):
            return str(int(n))
    except (ValueError, OverflowError):
        pass
    return s


def _find_uid_column(df: pd.DataFrame):
    """查找 Unified ID 列（可能列名有空格或大小写差异）。"""
    for cand in ["Unified ID", "Unified ID  ", "Unified_ID", "unified_id"]:
        for c in df.columns:
            if (c or "").strip().lower() == cand.strip().lower():
                return c
    return None


def run(year: int, week_tag: str) -> bool:
    """生成 final_join/{年}/{周}/ 下 target_strategy_old_with_ads_all.xlsx、target_strategy_new_with_ads_all.xlsx。"""
    target_dir = TARGET_BASE / str(year) / week_tag / "strategy_target"
    final_dir = FINAL_JOIN_BASE / str(year) / week_tag
    if not target_dir.exists():
        print(f"  跳过 build_final_join：未找到 {target_dir}")
        return True
    country_to_tier = load_country_to_tier()

    for key, filename, product_type in [
        ("old", "target_strategy_old.xlsx", "strategy_old"),
        ("new", "target_strategy_new.xlsx", "strategy_new"),
    ]:
        target_path = target_dir / filename
        if not target_path.exists():
            print(f"  跳过（不存在）: {target_path}")
            continue
        # 先读表头，确定 Unified ID 列名，再整表读取时将该列强制为字符串（避免 Excel 科学计数法）
        head_df = pd.read_excel(target_path, nrows=0)
        col_uid = _find_uid_column(head_df)
        converters = {col_uid: str} if col_uid else None
        target_df = pd.read_excel(target_path, converters=converters)
        col_product = "产品归属"
        if col_uid and col_uid in target_df.columns:
            target_df[col_uid] = target_df[col_uid].apply(_normalize_app_id)
            app_ids = target_df[col_uid].replace("", pd.NA).dropna().unique().tolist()
        else:
            if not col_uid:
                print(f"  ⚠️ {filename}: target 表缺少「Unified ID」列，无法匹配 country_data；4 市场获量将为 0（请确保步骤 1→2 产出的 target 含 Unified ID）")
            app_ids = target_df[col_product].astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist()
        app_ids = [x for x in app_ids if (x or "").strip()]
        if not app_ids:
            print(f"  {filename}: 无 app_id/产品归属，跳过")
            continue
        # 若 Unified ID 呈科学计数法（如 6.6e+23），无法匹配 JSON 文件名，会致获量为 0
        sci_ids = [aid for aid in app_ids if "e+" in (aid or "").lower() or "e-" in (aid or "").lower()]
        if sci_ids:
            print(f"  ⚠️ {filename}: 部分 Unified ID 为科学计数法（共 {len(sci_ids)} 个），无法匹配 country_data，这些行 4 市场获量为 0；建议在 target 表中将该列设为「文本」")
        # 检查是否有 country_data 可读（便于排查全 0）；优先 countiesdata/{年}/{周}/strategy_old|strategy_new、countiesdata/all、request/country_data
        n_with_data = sum(1 for aid in app_ids if load_country_data_for_app(aid, year=year, week_tag=week_tag, product_type=product_type))
        if n_with_data == 0:
            sample_dirs = [COUNTIESDATA_BASE / str(year) / week_tag / product_type / "json", LEGACY_COUNTRY_JSON]
            existing = []
            for d in sample_dirs:
                if d.exists():
                    existing.extend(list(d.glob("*.json"))[:3])
                    break
            sample_ids = app_ids[:3]
            print(f"  ⚠️ {filename}: 未找到任何地区数据（countiesdata 或 request/country_data），4 市场获量将为 0")
            print(f"     target 中 app_id 示例: {sample_ids}")
            if existing:
                print(f"     已有 json 示例: {[f.name for f in existing]}")
            print(f"     建议: 对该周先执行步骤 3（拉取地区数据），再执行 convert_country_json_to_xlsx，再执行步骤 5")
        else:
            print(f"  {filename}: 已匹配 {n_with_data}/{len(app_ids)} 个 app 的地区数据 → 写入四个市场获量")
        tier_df = build_tier_df(app_ids, country_to_tier, year=year, week_tag=week_tag, product_type=product_type)
        join_key = col_uid if col_uid in target_df.columns else col_product
        if col_uid in target_df.columns:
            target_df[join_key] = target_df[col_uid].apply(_normalize_app_id)
        else:
            target_df[join_key] = target_df[col_product].astype(str).str.strip()
        merged = target_df.merge(
            tier_df,
            left_on=join_key,
            right_on="app_id",
            how="left",
            suffixes=("", "_drop"),
        )
        merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_drop")], errors="ignore")
        if "app_id" in merged.columns and join_key != "app_id":
            merged = merged.drop(columns=["app_id"], errors="ignore")
        final_dir.mkdir(parents=True, exist_ok=True)
        out_name = f"target_strategy_{key}_with_ads_all.xlsx"
        out_path = final_dir / out_name
        merged.to_excel(out_path, index=False)
        print(f"  已写 final_join: {out_path}")
    # 同步生成前端用 JSON（产品维度页读 data/{年}/{周}/product_strategy_*.json）
    convert_script = BASE_DIR / "frontend" / "convert_final_join_to_json.py"
    if convert_script.exists():
        try:
            subprocess.run(
                [sys.executable, str(convert_script), "--year", str(year), "--week", week_tag],
                check=True,
                cwd=str(BASE_DIR),
            )
        except subprocess.CalledProcessError as e:
            print(f"  ⚠️ 生成前端 JSON 失败（退出码 {e.returncode}），产品维度页可能仍为旧数据")
    else:
        print(f"  ⚠️ 未找到 {convert_script}，跳过生成前端 JSON")
    return True


def main():
    parser = argparse.ArgumentParser(description="目标表 + 地区数据按 T 度汇总 → final_join")
    parser.add_argument("--year", type=int, required=True, help="年份")
    parser.add_argument("--week", dest="week_tag", required=True, help="周标签，如 1201-1207")
    args = parser.parse_args()
    ok = run(args.year, args.week_tag)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
