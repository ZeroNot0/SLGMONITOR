#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 newproducts/ 目录下的新游戏表（xlsx）转为 frontend/data/new_products.json，
供【产品维度】-【上线新游】页面展示。根据「开测时间」自动计算「所属周」（与 weeks_index 周段对应），
前端按左侧所选周过滤展示。
"""
import json
import re
from datetime import datetime
from pathlib import Path
import math

BASE_DIR = Path(__file__).resolve().parent.parent
NEWPRODUCTS_DIR = BASE_DIR / "newproducts"
DATA_DIR = Path(__file__).resolve().parent / "data"
OUT_JSON = DATA_DIR / "new_products.json"
WEEKS_INDEX_PATH = DATA_DIR / "weeks_index.json"

# 周标签格式: MMDD-MMDD，如 0112-0118、1229-0104（跨年）
WEEK_TAG_PATTERN = re.compile(r"^(\d{2})(\d{2})-(\d{2})(\d{2})$")

# 新产品监测表列顺序（表头可含这些列名，输出按此顺序）；无 Unified ID；所属周由脚本计算追加
COLUMNS_ORDER = [
    "产品名（实时更新中）",
    "产品归属",
    "题材",
    "画风",
    "发行商",
    "公司归属",
    "开测时间",
    "是否下架",
]


def _norm_cols(df):
    """列名去首尾空格便于匹配。"""
    return {str(c).strip(): c for c in df.columns if c is not None}


def week_tag_to_dates(year_str: str, week_tag: str):
    """将 (年, 周标签) 转为 (起始日 date, 结束日 date)。周标签如 0112-0118 -> 1月12日~1月18日。"""
    m = WEEK_TAG_PATTERN.match(week_tag.strip())
    if not m:
        return None, None
    sm, sd = int(m.group(1)), int(m.group(2))
    em, ed = int(m.group(3)), int(m.group(4))
    y = int(year_str)
    try:
        start_d = datetime(y, sm, sd).date()
    except ValueError:
        return None, None
    end_year = y + 1 if em < sm else y
    try:
        end_d = datetime(end_year, em, ed).date()
    except ValueError:
        return None, None
    return start_d, end_d


def parse_test_date(val) -> "datetime.date|None":
    """将开测时间解析为 date。支持：pandas Timestamp、2026/1/13、2026-01-13、2026.1.13。"""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if hasattr(val, "date"):
        return getattr(val, "date")()
    if isinstance(val, datetime):
        return val.date()
    s = str(val).strip()
    if not s:
        return None
    # 取前 10 位或到第一个空格
    s = s.split()[0][:10]
    for sep in ("-", "/", "."):
        if sep in s:
            parts = s.split(sep)
            if len(parts) >= 3:
                try:
                    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                    return datetime(y, m, d).date()
                except (ValueError, TypeError):
                    pass
            break
    return None


def load_week_ranges():
    """从 weeks_index.json 加载所有 (year, week_tag, start_date, end_date)。"""
    if not WEEKS_INDEX_PATH.exists():
        return []
    try:
        with open(WEEKS_INDEX_PATH, "r", encoding="utf-8") as f:
            index = json.load(f)
    except Exception:
        return []
    out = []
    for year in sorted(index.keys()):
        if year == "data_range" or not year.isdigit():
            continue
        for week_tag in sorted(index.get(year) or []):
            start_d, end_d = week_tag_to_dates(year, week_tag)
            if start_d is not None and end_d is not None:
                out.append((year, week_tag, start_d, end_d))
    return out


def run() -> bool:
    try:
        import pandas as pd
    except ImportError:
        print("  ⚠️ 需要 pandas、openpyxl，请安装: pip install pandas openpyxl")
        return False
    xlsx_files = list(NEWPRODUCTS_DIR.glob("*.xlsx")) if NEWPRODUCTS_DIR.exists() else []
    xlsx_files = [f for f in xlsx_files if not f.name.startswith(".") and not f.name.startswith("~")]
    if not xlsx_files:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump({"headers": [], "rows": []}, f, ensure_ascii=False, indent=2)
        print("  ⏭ newproducts/ 下无 xlsx，已写入空 new_products.json")
        return True
    # 取第一个 xlsx（可后续约定文件名如 新游戏.xlsx）
    excel_path = sorted(xlsx_files)[0]
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        print("  ⚠️ 无法读取 %s: %s" % (excel_path.name, e))
        return False
    if df.empty:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump({"headers": [], "rows": []}, f, ensure_ascii=False, indent=2)
        print("  ⏭ 表为空，已写入空 new_products.json")
        return True
    cols_norm = _norm_cols(df)
    # 输出列：按 COLUMNS_ORDER，存在的列才保留
    headers = [c for c in COLUMNS_ORDER if c in cols_norm]
    if not headers:
        headers = list(df.columns)
    else:
        df = df[[cols_norm[c] for c in headers]].copy()
        df.columns = headers
    rows = []
    for _, r in df.iterrows():
        row = []
        for v in r:
            if pd.isna(v):
                row.append("")
            elif isinstance(v, (int, float)) and not isinstance(v, bool):
                row.append(int(v) if v == int(v) else v)
            else:
                row.append(str(v).strip())
        rows.append(row)

    # 根据开测时间计算所属周（与 weeks_index 周段对应）
    week_ranges = load_week_ranges()
    col_open = headers.index("开测时间") if "开测时间" in headers else -1
    for row in rows:
        week_key = ""
        if col_open >= 0 and col_open < len(row):
            val = row[col_open]
            test_d = parse_test_date(val)
            if test_d:
                for year, week_tag, start_d, end_d in week_ranges:
                    if start_d <= test_d <= end_d:
                        week_key = "%s/%s" % (year, week_tag)
                        break
        row.append(week_key)
    headers = headers + ["所属周"]

    data = {"headers": headers, "rows": rows}
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("  已生成: %s（%d 行）" % (OUT_JSON.relative_to(BASE_DIR), len(rows)))
    return True


if __name__ == "__main__":
    import sys
    ok = run()
    sys.exit(0 if ok else 1)
