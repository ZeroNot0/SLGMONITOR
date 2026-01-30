#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
读取 frontend/data/{年}/{周}/metrics_total.json，在有「产品归属」的产品中按 All Time Downloads (WW)、All Time Revenue (WW)（累计数据）计算排名，
输出：
  - frontend/data/{年}/{周}/metrics_rank.json（当周目录，兼容）
  - frontend/data/metrics_rank.json（全局一份，所有页面统一用此排名）
格式：{ "Unified ID": { "rankInstall": n, "rankRevenue": m }, ... }
"""
import json
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(__file__).resolve().parent / "data"


def _parse_downloads(val):
    if val is None or val == "":
        return 0
    if isinstance(val, (int, float)):
        return float(val) if not (val != val) else 0  # NaN check
    s = re.sub(r"[,\s]", "", str(val))
    try:
        return float(s) if s else 0
    except ValueError:
        return 0


def _parse_revenue(val):
    if val is None or val == "":
        return 0.0
    if isinstance(val, (int, float)):
        return float(val) if not (val != val) else 0.0
    s = re.sub(r"[$,\s]", "", str(val))
    try:
        return float(s) if s else 0.0
    except ValueError:
        return 0.0


def build_metrics_rank(year: int, week_tag: str) -> bool:
    metrics_path = DATA_DIR / str(year) / week_tag / "metrics_total.json"
    if not metrics_path.exists():
        return False
    out_path = DATA_DIR / str(year) / week_tag / "metrics_rank.json"

    with open(metrics_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    headers = data.get("headers") or []
    rows = data.get("rows") or []

    idx_unified = next((i for i, h in enumerate(headers) if h == "Unified ID"), -1)
    idx_product = next((i for i, h in enumerate(headers) if h == "产品归属"), -1)
    idx_downloads = next((i for i, h in enumerate(headers) if h == "All Time Downloads (WW)"), -1)
    idx_revenue = next((i for i, h in enumerate(headers) if h == "All Time Revenue (WW)"), -1)
    if idx_unified < 0 or idx_product < 0 or (idx_downloads < 0 and idx_revenue < 0):
        return False

    seen = set()
    products = []
    for row in rows:
        if len(row) <= max(idx_unified, idx_product):
            continue
        uid = (row[idx_unified] or "").strip() if idx_unified >= 0 else ""
        pname = (row[idx_product] or "").strip() if idx_product >= 0 else ""
        if not uid or not pname:
            continue
        if uid in seen:
            continue
        seen.add(uid)
        downloads = _parse_downloads(row[idx_downloads] if idx_downloads >= 0 and idx_downloads < len(row) else None)
        revenue = _parse_revenue(row[idx_revenue] if idx_revenue >= 0 and idx_revenue < len(row) else None)
        products.append({"unifiedId": uid, "downloads": downloads, "revenue": revenue})

    by_downloads = sorted(products, key=lambda x: x["downloads"], reverse=True)
    by_revenue = sorted(products, key=lambda x: x["revenue"], reverse=True)
    rank_install = {p["unifiedId"]: (i + 1) for i, p in enumerate(by_downloads)}
    rank_revenue = {p["unifiedId"]: (i + 1) for i, p in enumerate(by_revenue)}

    result = {}
    for uid in rank_install.keys():
        result[uid] = {
            "rankInstall": rank_install.get(uid, 0),
            "rankRevenue": rank_revenue.get(uid, 0),
        }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    # 全局一份，供所有产品详情页统一使用（累计数据排名）
    global_path = DATA_DIR / "metrics_rank.json"
    with open(global_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 已生成: {out_path.relative_to(BASE_DIR)} 与 {global_path.relative_to(BASE_DIR)}（{len(result)} 个产品）")
    return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description="根据 metrics_total.json 计算赛道排名，生成 metrics_rank.json")
    parser.add_argument("--year", type=int, required=True, help="年份，如 2026")
    parser.add_argument("--week", type=str, required=True, help="周标签，如 0112-0118")
    args = parser.parse_args()
    if not build_metrics_rank(args.year, args.week):
        print(f"  ⏭ 未找到或无法解析 data/{args.year}/{args.week}/metrics_total.json，跳过")
        raise SystemExit(0)


if __name__ == "__main__":
    main()
