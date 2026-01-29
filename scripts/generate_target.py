#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能一·获得目标产品：从数据监测表（output 表）中按规则筛出策略目标与非策略目标，写入 target/ 下。

- 策略目标：仅通过产品归属表确定——产品归属在 mapping/产品归属.xlsx 中的产品即为策略目标。
- 非策略目标：通过 P50 + 周安装变动 > 20% 确定——非策略产品中，当周周安装 ≥ P50 且 周安装变动 > 20% 的行。

old / new 划分：按「第三方记录最早上线时间」与截止日期（默认 2025-01-01）区分爆量旧产品 / 新产品。
策略目标另需满足：当周周安装 ≥ 1000。
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
MAPPING_DIR = BASE_DIR / "mapping"

# 爆量旧产品 / 新产品 划分：上线时间早于该日期的为 old，否则为 new
OLD_NEW_CUTOFF_DATE = "2025-01-01"


def _parse_install_change(s):
    """从「周安装变动」字符串（如 +35.12%▲）解析出数值（如 35.12），无法解析返回 None。"""
    if pd.isna(s) or s is None:
        return None
    s = str(s).strip()
    m = re.search(r"([+-]?\d+\.?\d*)\s*%", s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _parse_earliest_date(v):
    """将「第三方记录最早上线时间」转为可比较的日期字符串或 None。"""
    if pd.isna(v) or v is None:
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    if not s:
        return None
    # Excel 序列号
    try:
        n = float(s.replace(",", ""))
        if 40000 <= n <= 50000:
            from datetime import datetime
            from datetime import timedelta
            base = datetime(1899, 12, 30)
            d = base + timedelta(days=int(n))
            return d.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass
    # 已有 YYYY-MM-DD 或 YYYY/MM/DD
    for sep in ["-", "/", "."]:
        if sep in s:
            parts = re.split(r"[-/.]", s)
            if len(parts) >= 3:
                try:
                    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                    if 2000 <= y <= 2030 and 1 <= m <= 12 and 1 <= d <= 31:
                        return f"{y:04d}-{m:02d}-{d:02d}"
                except (ValueError, TypeError):
                    pass
    return None


def get_strategy_product_set():
    """从 mapping/产品归属.xlsx 读取「产品归属」列，作为策略产品集合（表中出现的即为策略目标）。"""
    path = MAPPING_DIR / "产品归属.xlsx"
    if not path.exists():
        raise FileNotFoundError(f"未找到: {path}")
    df = pd.read_excel(path)
    # step2 使用 B 列(1) 与 E 列(4)，E 列为产品归属
    if df.shape[1] >= 5:
        col = df.iloc[:, 4]
    else:
        col = df.iloc[:, 0]
    return set(col.dropna().astype(str).str.strip().unique())


def load_monitor_table(week_tag: str, year: int):
    """加载数据监测表：优先 output/{年}/{周}_SLG数据监测表.xlsx，否则 intermediate/{年}/{周}/pivot_table.xlsx 并计算周安装变动。"""
    out_file = BASE_DIR / "output" / str(year) / f"{week_tag}_SLG数据监测表.xlsx"
    if out_file.exists():
        return pd.read_excel(out_file)
    pivot_file = BASE_DIR / "intermediate" / str(year) / week_tag / "pivot_table.xlsx"
    if not pivot_file.exists():
        raise FileNotFoundError(f"未找到数据监测表或 pivot: {out_file} 或 {pivot_file}")
    df = pd.read_excel(pivot_file)
    if "周安装变动" not in df.columns and "当周周安装" in df.columns and "上周周安装" in df.columns:
        denom = df["上周周安装"].replace(0, pd.NA)
        pct = (df["当周周安装"] - df["上周周安装"]) / denom
        df["周安装变动"] = pct.apply(lambda x: f"{x*100:.2f}%▲" if pd.notna(x) and x >= 0 else f"{x*100:.2f}%▼" if pd.notna(x) else "")
    return df


def run_generate_target(week_tag: str, year: int, old_new_cutoff: str = OLD_NEW_CUTOFF_DATE) -> None:
    strategy_set = get_strategy_product_set()
    df = load_monitor_table(week_tag, year)

    # 去掉「公司汇总」行（公司归属以「汇总」结尾）
    col_company = "公司归属"
    if col_company in df.columns:
        df = df[df[col_company].astype(str).str.strip().str.endswith("汇总") == False].copy()

    col_product = "产品归属"
    col_install = "当周周安装"
    col_change = "周安装变动"
    col_date = "第三方记录最早上线时间"
    if col_product not in df.columns:
        raise ValueError("数据监测表缺少列: 产品归属")

    # 策略目标：产品归属在产品归属表中，且当周周安装 ≥ 1000
    strategy_mask = df[col_product].astype(str).str.strip().isin(strategy_set)
    if col_install in df.columns:
        inst = pd.to_numeric(df[col_install], errors="coerce").fillna(0)
        strategy_mask = strategy_mask & (inst >= 1000)
    strategy_df = df[strategy_mask].copy()

    # 非策略：产品归属不在策略表中的行
    non_strategy_df = df[~strategy_mask].copy()
    # P50 + 周安装变动 > 20%
    if col_install in non_strategy_df.columns and col_change in non_strategy_df.columns and len(non_strategy_df) > 0:
        installs = pd.to_numeric(non_strategy_df[col_install], errors="coerce").fillna(0)
        p50 = installs.median()
        pct_vals = non_strategy_df[col_change].apply(_parse_install_change)
        mask_p50 = installs >= p50
        mask_20 = pct_vals.notna() & (pct_vals > 20)
        non_strategy_df = non_strategy_df[mask_p50 & mask_20].copy()
    else:
        non_strategy_df = non_strategy_df.iloc[0:0]

    def split_old_new(sub_df):
        if col_date not in sub_df.columns or sub_df.empty:
            return sub_df, sub_df.iloc[0:0]
        dates = sub_df[col_date].apply(_parse_earliest_date)
        old_mask = (dates < old_new_cutoff) | (dates.isna())
        return sub_df[old_mask].copy(), sub_df[~old_mask].copy()

    strategy_old, strategy_new = split_old_new(strategy_df)
    non_old, non_new = split_old_new(non_strategy_df)

    out_dir = BASE_DIR / "target" / str(year) / week_tag
    (out_dir / "strategy_target").mkdir(parents=True, exist_ok=True)
    (out_dir / "non_strategy_target").mkdir(parents=True, exist_ok=True)

    strategy_old.to_excel(out_dir / "strategy_target" / "target_strategy_old.xlsx", index=False)
    strategy_new.to_excel(out_dir / "strategy_target" / "target_strategy_new.xlsx", index=False)
    non_old.to_excel(out_dir / "non_strategy_target" / "target_non_strategy_old.xlsx", index=False)
    non_new.to_excel(out_dir / "non_strategy_target" / "target_non_strategy_new.xlsx", index=False)

    print(f"  策略目标 old: {len(strategy_old)} 行, new: {len(strategy_new)} 行")
    print(f"  非策略目标 old: {len(non_old)} 行, new: {len(non_new)} 行")
    print(f"  输出: {out_dir}")


def main():
    parser = argparse.ArgumentParser(description="功能一·获得目标产品，写入 target/{年}/{周}/")
    parser.add_argument("--week", required=True, help="周标签，如 1201-1207 或 0119-0125")
    parser.add_argument("--year", type=int, required=True, help="年份")
    parser.add_argument("--old_new_cutoff", default=OLD_NEW_CUTOFF_DATE, help="old/new 划分截止日期，默认 2025-01-01")
    args = parser.parse_args()
    run_generate_target(args.week, args.year, args.old_new_cutoff)


if __name__ == "__main__":
    main()
