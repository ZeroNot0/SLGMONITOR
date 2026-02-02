#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将上传的产品/公司归属表合并进 mapping/ 总表。

上传表必须包含列：产品名（实时更新中）、Unified ID、产品归属、题材、画风、发行商；公司归属可选，
若缺少或为空则通过【发行商】在 mapping/公司归属.xlsx 中查找填充。
合成新表包含 7 列；按 Unified ID 合并时，若已有行与上传行在任一一列不同则用上传行更新（视为新表一条记录）。

- 更新 mapping/产品归属.xlsx：按 Unified ID 合并（已有且 7 列全同则保留一条，任一一列不同则用上传行更新），列顺序满足 step2 iloc[:, [1,4]]。
- 更新 mapping/公司归属.xlsx：从上传表取 (发行商, 公司归属) 去重后写入，列顺序满足 step2 iloc[:, [1,2]]。
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
MAPPING_DIR = BASE_DIR / "mapping"
PROD_XLSX = MAPPING_DIR / "产品归属.xlsx"
COMP_XLSX = MAPPING_DIR / "公司归属.xlsx"

# 上传表必填列（缺一不可）；公司归属可选，缺则用发行商从公司归属表查
REQUIRED_UPLOAD = [
    "产品名（实时更新中）",
    "Unified ID",
    "产品归属",
    "题材",
    "画风",
    "发行商",
]
# 输出产品归属表固定 7 列
OUTPUT_COLUMNS = [
    "产品名（实时更新中）",
    "Unified ID",
    "产品归属",
    "题材",
    "画风",
    "发行商",
    "公司归属",
]


def _norm_cols(df):
    """列名去首尾空格便于匹配。"""
    return {str(c).strip(): c for c in df.columns if c is not None}


def _get_cell(row, old_cols, candidates):
    """从 row 中按候选列名取第一个存在的列的值。"""
    for name in candidates:
        if name in old_cols:
            try:
                v = row[old_cols[name]]
            except (KeyError, IndexError):
                return ""
            if v is None or (isinstance(v, float) and str(v) == "nan"):
                return ""
            return str(v).strip()
    return ""


def run(upload_path: Path) -> tuple:
    """
    执行合并。upload_path 为上传的 Excel 路径。
    返回 (success: bool, message: str)。
    """
    try:
        import pandas as pd
    except ImportError:
        return False, "需要 pandas、openpyxl，请安装: pip install pandas openpyxl"
    if not upload_path.exists() or not upload_path.is_file():
        return False, "上传文件不存在"
    try:
        df_up = pd.read_excel(upload_path)
    except Exception as e:
        return False, "无法读取上传文件（%s）" % str(e)[:80]
    if df_up.empty:
        return False, "上传表为空"
    cols_norm = _norm_cols(df_up)
    missing = [c for c in REQUIRED_UPLOAD if c not in cols_norm]
    if missing:
        return False, "缺少必填列，无法执行：" + "、".join(missing)
    # 取必填列 + 公司归属（若有）；公司归属缺则先填空再按发行商从公司归属表补
    take_cols = list(REQUIRED_UPLOAD)
    if "公司归属" in cols_norm:
        take_cols = take_cols + ["公司归属"]
    df_up = df_up[[cols_norm[c] for c in take_cols]].copy()
    df_up.columns = take_cols
    if "公司归属" not in df_up.columns:
        df_up["公司归属"] = ""
    else:
        df_up["公司归属"] = df_up["公司归属"].astype(str).str.strip()
    # 通过发行商从公司归属表补全缺失的公司归属
    publisher_to_company = {}
    if COMP_XLSX.exists():
        try:
            df_comp = pd.read_excel(COMP_XLSX)
            if not df_comp.empty and df_comp.shape[1] >= 2:
                for _, r in df_comp.iterrows():
                    pub = str(r.iloc[1]).strip() if pd.notna(r.iloc[1]) else ""
                    comp = str(r.iloc[2]).strip() if pd.notna(r.iloc[2]) else ""
                    if pub and pub not in publisher_to_company:
                        publisher_to_company[pub] = comp
        except Exception:
            pass
    def fill_company(row):
        if pd.isna(row["公司归属"]) or str(row["公司归属"]).strip() == "":
            pub = str(row["发行商"]).strip() if pd.notna(row["发行商"]) else ""
            return publisher_to_company.get(pub, "")
        return str(row["公司归属"]).strip()
    df_up["公司归属"] = df_up.apply(fill_company, axis=1)
    df_up = df_up[OUTPUT_COLUMNS].copy()
    # 产品归属表：列顺序 序号, 产品名（实时更新中）, Unified ID, 题材, 产品归属, 画风, 发行商, 公司归属 → step2 用 [1,4] = 产品名、产品归属
    if PROD_XLSX.exists():
        try:
            df_old = pd.read_excel(PROD_XLSX)
        except Exception:
            df_old = pd.DataFrame()
    else:
        df_old = pd.DataFrame()
    if not df_old.empty and df_old.shape[1] >= 5:
        old_cols = _norm_cols(df_old)
        key_col = None
        for k in ("Unified ID", "Unified id", "unified id"):
            if k in old_cols:
                key_col = old_cols[k]
                break
        if key_col is not None:
            df_old[key_col] = df_old[key_col].astype(str).str.strip()
            df_up["Unified ID"] = df_up["Unified ID"].astype(str).str.strip()
            upload_ids = set(df_up["Unified ID"].dropna().unique())
            df_old_rest = df_old[~df_old[key_col].isin(upload_ids)]
            if not df_old_rest.empty:
                # 将旧表剩余行映射到必填列（缺列填空）
                def row_to_required(row):
                    return {
                        "产品名（实时更新中）": _get_cell(row, old_cols, ["产品名（实时更新中）", "Unified Name", "产品名"]),
                        "Unified ID": _get_cell(row, old_cols, ["Unified ID"]),
                        "产品归属": _get_cell(row, old_cols, ["产品归属"]),
                        "题材": _get_cell(row, old_cols, ["题材", "题材标签"]),
                        "画风": _get_cell(row, old_cols, ["画风", "画风标签"]),
                        "发行商": _get_cell(row, old_cols, ["发行商"]),
                        "公司归属": _get_cell(row, old_cols, ["公司归属"]),
                    }
                old_rows = [row_to_required(row) for _, row in df_old_rest.iterrows()]
                df_old_mapped = pd.DataFrame(old_rows)
                df_old_mapped = df_old_mapped[OUTPUT_COLUMNS]
                df_merged = pd.concat([df_old_mapped, df_up], ignore_index=True)
            else:
                df_merged = df_up.copy()
        else:
            df_merged = df_up.copy()
    else:
        df_merged = df_up.copy()
    # 输出产品归属表：列顺序满足 step2 iloc[:, [1, 4]]
    out_prod = df_merged[OUTPUT_COLUMNS].copy()
    out_prod.insert(0, "序号", range(1, len(out_prod) + 1))
    MAPPING_DIR.mkdir(parents=True, exist_ok=True)
    out_prod.to_excel(PROD_XLSX, index=False)
    # 公司归属表：从上传表取 (发行商, 公司归属) 去重；若已有表则合并去重
    comp_pairs = df_up[["发行商", "公司归属"]].drop_duplicates()
    comp_pairs = comp_pairs[comp_pairs["发行商"].notna() & comp_pairs["公司归属"].notna()]
    if COMP_XLSX.exists():
        try:
            df_comp_old = pd.read_excel(COMP_XLSX)
            if not df_comp_old.empty and df_comp_old.shape[1] >= 2:
                c1 = df_comp_old.iloc[:, 1]
                c2 = df_comp_old.iloc[:, 2]
                comp_pairs_old = pd.DataFrame({"发行商": c1, "公司归属": c2}).drop_duplicates()
                comp_pairs = pd.concat([comp_pairs_old, comp_pairs]).drop_duplicates(subset=["发行商"], keep="last")
        except Exception:
            pass
    out_comp = comp_pairs.reset_index(drop=True)
    out_comp.insert(0, "序号", range(1, len(out_comp) + 1))
    out_comp.to_excel(COMP_XLSX, index=False)
    return True, "产品/公司归属表已更新（产品归属 %d 条，公司归属 %d 条）" % (len(out_prod), len(out_comp))


if __name__ == "__main__":
    import sys
    p = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if not p:
        print("用法: python update_mapping_from_upload.py <上传的Excel路径>")
        sys.exit(1)
    ok, msg = run(p)
    print(msg)
    sys.exit(0 if ok else 1)
